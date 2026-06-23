# Workflow Wait-Family — Checkpoint 3 (Signals) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement task-by-task. Checkbox (`- [ ]`) steps.

**Goal:** Add River Pro workflow **signals** — external/human-in-the-loop events that satisfy a task's wait. `Workflow.Signals().Emit(...)` writes an idempotent, queryable signal; the scheduler loads signals and feeds them into `waiteval` so signal terms (`WaitTermSignal`) finally resolve. Closes CP2's empty-signals stub.

**Architecture:** Signals live in a new dedicated table `river_workflow_signal` (idempotency via a unique index, queryable for audit). A driver `WorkflowSignalEmit` enforces idempotency + payload-mismatch detection; `WorkflowSignalList` reads them. The public `Workflow.Signals()` builds `Emit`/`List`/`LatestForTask`/`ListForTask` on those two driver methods. The scheduler's `evaluateWaits` loads each waiting task's workflow signals and populates `waiteval.Inputs.Signals` (now carrying full signal metadata: payload/attempt/created_at/id/source).

**Tech Stack:** Go 1.25, sqlc (Postgres + SQLite), migrations, `riverworkflow`, `riverdriver`, `riverdrivertest`, `rivertype`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-23-workflow-wait-family-design.md` §6 (storage), §3 (signals API), §7 (driver). CP1+CP2 are merged on `master`.
- **API parity (River Pro):** `w.Signals().Emit(ctx, key string, payload any, *WorkflowSignalEmitOpts{IdempotencyKey, Source})`; `LatestForTask`/`ListForTask`/`List`; `SignalPayloadMismatchError`. Signal-term CEL scope: `payload, attempt, created_at, id, source`.
- **Three drivers in lockstep:** migrations added to ALL THREE `migration/main/` dirs (pgx + databasesql = Postgres-identical; sqlite = dialect). SQL queries: Postgres source in `riverpgxv5/internal/dbsqlc` (regenerates pgx+databasesql), SQLite in `riversqlite/internal/dbsqlc`. `make generate/sqlc`; `make verify/sqlc` diff-clean.
- **Idempotency contract:** `(workflow_id, idempotency_key)` unique (only when idempotency_key set). Re-emit same key + identical payload → no-op, returns existing. Same key + different payload → `ErrSignalPayloadMismatch`. No idempotency_key → always inserts.
- **Determinism:** `waiteval` stays pure; the scheduler passes signal data in. Migrate up+down both verified; new table follows the `008_durable_periodic_jobs` precedent (`/* TEMPLATE: schema */` table prefix; dialect: Postgres `timestamptz`/`now()`/`jsonb`/`bigserial`; SQLite `timestamp`/`CURRENT_TIMESTAMP`/`text`/`integer PRIMARY KEY AUTOINCREMENT`).
- DB available: Postgres running (`postgres://localhost/river_test?sslmode=disable`), `sqlc` on PATH, SQLite in-process. Conformance lives in `riverdriver/riverdrivertest`.

---

### Task 1: Migration `009_workflow_signals` (three drivers)

**Files:**
- Create (Postgres, identical content): `riverdriver/riverpgxv5/migration/main/009_workflow_signals.{up,down}.sql` AND `riverdriver/riverdatabasesql/migration/main/009_workflow_signals.{up,down}.sql`
- Create (SQLite): `riverdriver/riversqlite/migration/main/009_workflow_signals.{up,down}.sql`
- Test: rely on the existing migration test harness (each driver's migrate test applies all migrations); add a focused up/down check if the harness supports it.

**Interfaces:** Produces the `river_workflow_signal` table consumed by Task 2.

- [ ] **Step 1: Write the Postgres up migration** (pgx + databasesql, identical):

```sql
CREATE TABLE /* TEMPLATE: schema */river_workflow_signal (
    id bigserial PRIMARY KEY,
    workflow_id text NOT NULL,
    signal_key text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key text,
    source text,
    created_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    CONSTRAINT river_workflow_signal_workflow_id_length CHECK (char_length(workflow_id) > 0 AND char_length(workflow_id) < 128),
    CONSTRAINT river_workflow_signal_signal_key_length CHECK (char_length(signal_key) > 0 AND char_length(signal_key) < 128)
);
CREATE UNIQUE INDEX river_workflow_signal_idempotency ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX river_workflow_signal_lookup ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, signal_key, created_at);
```
Down: `DROP TABLE IF EXISTS /* TEMPLATE: schema */river_workflow_signal;`

- [ ] **Step 2: Write the SQLite up migration:**

```sql
CREATE TABLE /* TEMPLATE: schema */river_workflow_signal (
    id integer PRIMARY KEY AUTOINCREMENT,
    workflow_id text NOT NULL,
    signal_key text NOT NULL,
    payload text NOT NULL DEFAULT '{}',
    idempotency_key text,
    source text,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at timestamp,
    CONSTRAINT river_workflow_signal_workflow_id_length CHECK (length(workflow_id) > 0 AND length(workflow_id) < 128),
    CONSTRAINT river_workflow_signal_signal_key_length CHECK (length(signal_key) > 0 AND length(signal_key) < 128)
);
CREATE UNIQUE INDEX river_workflow_signal_idempotency ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX river_workflow_signal_lookup ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, signal_key, created_at);
```
Down: same DROP.

- [ ] **Step 3: Verify migrations apply** on all three drivers. Find the existing migration test (e.g. a `rivermigrate` test or the driver test that runs `Migrate`), run it: the `riverdbtest.TestSchema` path used by conformance applies all migrations to a fresh schema, so `go test ./riverdriver/riverdrivertest/ -run TestDriverRiverPgxV5 -count=1` exercising any test confirms 009 applies. Confirm up AND down (if a down-migration test exists). Manually sanity-check with `psql`/`sqlite3` if needed.

- [ ] **Step 4: Commit**

```bash
git add riverdriver/*/migration/main/009_workflow_signals.*.sql
git commit -m "Add 009_workflow_signals migration across drivers"
```

---

### Task 2: Signal type + driver methods (`WorkflowSignalEmit`, `WorkflowSignalList`) + conformance

**Files:**
- Modify: `rivertype/river_type.go` (add `WorkflowSignal` struct + `ErrWorkflowSignalPayloadMismatch` sentinel) — OR `riverdriver` if rivertype isn't the right home; follow where `JobRow` and existing sentinels live.
- Modify: `riverdriver/river_driver_interface.go` (2 methods + param structs)
- Modify: `riverpgxv5/internal/dbsqlc/river_workflow_signal.sql` (NEW query file) + `sqlc.yaml` (add the file), `riversqlite/internal/dbsqlc/river_workflow_signal.sql` (+ its sqlc.yaml)
- Regenerate: the three `river_workflow_signal.sql.go`
- Modify: 3 driver main files (wrappers)
- Test: `riverdriver/riverdrivertest/workflow_signal.go` (NEW) + register in `riverdrivertest.go`

**Interfaces (produced; consumed by Tasks 4 & 5):**
- `rivertype.WorkflowSignal struct { ID int64; WorkflowID, SignalKey string; Payload []byte; IdempotencyKey, Source *string; CreatedAt time.Time; ResolvedAt *time.Time }`
- `var rivertype.ErrWorkflowSignalPayloadMismatch = errors.New("riverworkflow: signal idempotency key reused with a different payload")`
- `Executor.WorkflowSignalEmit(ctx, *WorkflowSignalEmitParams) (*rivertype.WorkflowSignal, error)`; `WorkflowSignalEmitParams{ WorkflowID, SignalKey string; Payload []byte; IdempotencyKey, Source *string; Now time.Time; Schema string }`. Idempotency: insert; on `(workflow_id, idempotency_key)` conflict, read the existing row — if its payload equals the new payload, return it (no-op); else return `ErrWorkflowSignalPayloadMismatch`.
- `Executor.WorkflowSignalList(ctx, *WorkflowSignalListParams) ([]*rivertype.WorkflowSignal, error)`; `WorkflowSignalListParams{ WorkflowID string; SignalKey *string; Max int; Schema string }` — filter by workflow (and key if set), `ORDER BY created_at, id`, limit Max (apply the design's scan-limit default in the caller; the driver just honors Max).

- [ ] **Step 1: Write the failing conformance exercise** in `riverdriver/riverdrivertest/workflow_signal.go`:
  - `Emit_InsertsAndReturns`: emit → returns row with id/payload/created_at.
  - `Emit_IdempotentNoOp`: emit twice same idempotency_key + same payload → second returns the SAME id, only one row in List.
  - `Emit_PayloadMismatch`: emit same key + different payload → `errors.Is(err, rivertype.ErrWorkflowSignalPayloadMismatch)`.
  - `Emit_NoIdempotencyKeyAlwaysInserts`: two emits, nil idempotency_key → two rows.
  - `List_FiltersAndOrders`: emit several across keys → List by workflow returns all ordered; List with SignalKey filters.
  Register in `Exercise()`. Run on pgx → FAIL (methods undefined).

- [ ] **Step 2: Add the type + sentinel** (`rivertype`), the interface methods + param structs (`river_driver_interface.go`).

- [ ] **Step 3: Write SQL** in a new `river_workflow_signal.sql` per Postgres/SQLite dialect. Emit (Postgres) sketch:
```sql
-- name: WorkflowSignalEmit :one
INSERT INTO /* TEMPLATE: schema */river_workflow_signal (workflow_id, signal_key, payload, idempotency_key, source, created_at)
VALUES (@workflow_id, @signal_key, @payload, sqlc.narg('idempotency_key'), sqlc.narg('source'), @now::timestamptz)
ON CONFLICT (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *;
-- name: WorkflowSignalGetByIdempotency :one
SELECT * FROM /* TEMPLATE: schema */river_workflow_signal WHERE workflow_id = @workflow_id AND idempotency_key = @idempotency_key;
-- name: WorkflowSignalList :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id AND (sqlc.narg('signal_key')::text IS NULL OR signal_key = sqlc.narg('signal_key'))
ORDER BY created_at, id LIMIT @max::int;
```
The Go wrapper implements the idempotency-with-mismatch logic: call Emit; if it returns no row (conflict), call GetByIdempotency, compare `payload` bytes (normalize via `json` equality if needed) → return existing or `ErrWorkflowSignalPayloadMismatch`. SQLite: `INSERT ... ON CONFLICT(...) DO NOTHING RETURNING *` is supported in modern SQLite; mirror with `json_extract`-free direct columns; the partial-unique-index conflict target works.

- [ ] **Step 4: Regenerate** (`make generate/sqlc`; add the new `.sql` to each `sqlc.yaml` queries list first) and verify clean. Implement the 3 wrappers (map rows to `rivertype.WorkflowSignal`).

- [ ] **Step 5: Conformance green on all three drivers.** Run the exercise on pgx, sqlite, databasesql.

- [ ] **Step 6: Commit**
```bash
git add rivertype/ riverdriver/
git commit -m "Add WorkflowSignalEmit/List driver methods with idempotency + payload-mismatch"
```

---

### Task 3: `waiteval` — full signal metadata in `Inputs.Signals`

**Files:** Modify `riverworkflow/internal/waiteval/waiteval.go`, `waiteval_test.go`.

**Interfaces:**
- Change `Inputs.Signals` from `map[string]any` to `map[string]SignalView` where `type SignalView struct { Payload any; Attempt int; CreatedAt time.Time; ID int64; Source string }`.
- The signal sub-environment exposes `payload` (= `SignalView.Payload`), `attempt`, `created_at` (as a CEL timestamp or unix seconds — pick one, document), `id`, `source`. Replace the CP2 `// PARITY: full signal metadata wired in CP3` stub with the real bindings.
- Absent key → term false (unchanged). Empty-CELExpr signal term → presence gate (unchanged).

- [ ] **Step 1: Update the existing signal tests** to the new `SignalView` shape and ADD tests: a signal term `payload.ok && attempt > 0` evaluates against a populated `SignalView{Payload: map[string]any{"ok":true}, Attempt:1}` → true; `source == "api"` term resolves from `SignalView.Source`. Confirm RED (compile error on the type change), implement, GREEN.

- [ ] **Step 2: Implement** the type change + sub-env bindings. Update `Evaluate` to read from `SignalView`. Keep purity.

- [ ] **Step 3: Full waiteval package green; build the parent (`riverworkflow`) — it will fail to compile until Task 5 updates the scheduler caller.** That's expected; note it. Commit waiteval alone (it compiles in isolation as a package; the parent build break is fixed in Task 5).

> Sequencing note: Task 3 changes `Inputs.Signals`'s type, which the scheduler (Task 5) consumes. Do Task 3 then Task 5; between them `riverworkflow` top-level won't build. Run `cd riverworkflow && go test ./internal/waiteval/` (passes in isolation). Task 5 restores the full build.

- [ ] **Step 4: Commit**
```bash
git add riverworkflow/internal/waiteval/
git commit -m "Wire full signal metadata (payload/attempt/created_at/id/source) into waiteval"
```

---

### Task 4: Public `Workflow.Signals()` API

**Files:** Create `riverworkflow/signals.go`, `riverworkflow/signals_test.go`. Modify `riverworkflow/errors.go` (re-export sentinel) if desired.

**Interfaces (Pro-parity):**
- `func (w *Workflow[TTx]) Signals() *WorkflowSignals[TTx]`
- `type WorkflowSignals[TTx any]` bound to the workflow id + driver/exec + schema.
- `func (s *WorkflowSignals[TTx]) Emit(ctx, key string, payload any, opts *WorkflowSignalEmitOpts) (*rivertype.WorkflowSignal, error)` (+ `EmitTx(ctx, tx, ...)`). Marshals `payload` to JSON, calls `exec.WorkflowSignalEmit`. Returns `ErrSignalPayloadMismatch` (re-exported from rivertype) on mismatch.
- `type WorkflowSignalEmitOpts struct { IdempotencyKey string; Source string }`
- `func (s *WorkflowSignals[TTx]) List(ctx, *WorkflowSignalListParams) ([]*rivertype.WorkflowSignal, error)` — workflow-wide.
- `func (s *WorkflowSignals[TTx]) ListForTask(ctx, taskName, key string, *WorkflowSignalListForTaskParams) (...)` and `LatestForTask(ctx, taskName, key string, *WorkflowSignalLatestForTaskOpts) (*rivertype.WorkflowSignal, error)` — built on `WorkflowSignalList` filtered by key; `Latest` = last by created_at. For CP3, `taskName` is accepted for API parity but the filter is workflow+key (the per-task resolution view / `IncludeAfterResolution` is CP4); document this. Apply the design's `SignalScanLimit` default (10_000) as `Max`.
- `var ErrSignalPayloadMismatch = rivertype.ErrWorkflowSignalPayloadMismatch` (re-export for the documented `riverworkflow.SignalPayloadMismatchError` name).

- [ ] **Step 1: Failing tests** (`signals_test.go`): construct a `WorkflowSignals` against a real driver+schema (use the simulation/test harness DB pattern — pgx + `riverdbtest.TestSchema`), Emit a signal, List it back, assert idempotency no-op and mismatch error surface through the public API. Round-trip the payload (emit `map[string]any{"ok":true}`, read back, unmarshal).

- [ ] **Step 2: Implement `signals.go`.** `Emit` marshals payload, builds `WorkflowSignalEmitParams` (IdempotencyKey/Source as `*string` when non-empty), calls exec. `List`/`ListForTask`/`LatestForTask` call `WorkflowSignalList` and post-filter.

- [ ] **Step 3: Tests green; `cd riverworkflow && go test ./...`** (note: depends on Task 5 if signals.go references the scheduler — it shouldn't; signals.go only uses driver methods, so it builds independently).

- [ ] **Step 4: Commit**
```bash
git add riverworkflow/signals.go riverworkflow/signals_test.go riverworkflow/errors.go
git commit -m "Add Workflow.Signals() public API (Emit/List/LatestForTask/ListForTask)"
```

---

### Task 5: Scheduler loads signals into evaluation

**Files:** Modify `riverworkflow/internal/workflowscheduler/wait_eval.go` (+ its test); ensure `riverworkflow` builds again after Task 3's type change.

**Interfaces:** Consumes `exec.WorkflowSignalList` and `waiteval.SignalView`.

- [ ] **Step 1: Implement.** In `evaluateWaits`/`processWaitTask`, for a Satisfied-deps task with a WaitSpec that has signal terms (or always), call `exec.WorkflowSignalList(ctx, {WorkflowID, Max: scanLimit})`, and build `Inputs.Signals map[string]SignalView` keyed by `signal_key` → the LATEST signal for that key (`Payload` = unmarshalled JSON, `CreatedAt`, `ID`, `Source` from the row; `Attempt` = count of signals seen for that key, or 1 — document the inferred meaning with `// PARITY: attempt = emission count, inferred`). Pass into `Evaluate`. Add a `SignalScanLimit` to the scheduler `Config`, default 10_000, threaded from `riverworkflow.Config.SignalScanLimit`.

- [ ] **Step 2: Integration test** in `simulation_test.go`: a task gated by `WaitTermSignal("approved", "approved", "payload.ok")` with `Expr:"approved"`; start the client; assert the task stays pending; `w.Signals().Emit(ctx, "approved", map[string]any{"ok":true}, nil)`; assert the task then promotes/executes. Use the existing pgx harness.

- [ ] **Step 3: `cd riverworkflow && go test ./...`** fully green (full build restored); `go build ./...` clean.

- [ ] **Step 4: Commit**
```bash
git add riverworkflow/internal/workflowscheduler/ riverworkflow/config.go riverworkflow/client.go
git commit -m "Load workflow signals into wait evaluation; signal-gated tasks now promote"
```

---

### Task 6: End-to-end human-approval example

**Files:** Create `riverworkflow/example_workflow_signal_test.go`.

- [ ] **Step 1: Example** demonstrating a two-task workflow where task 2 waits on a `WaitTermSignal` approval; the example emits the signal and the workflow completes, printing one deterministic line (`// Output: approved`). Follow the CP2 timer example's deterministic-output approach. Run 3× for stability.

- [ ] **Step 2: Commit**
```bash
git add riverworkflow/example_workflow_signal_test.go
git commit -m "Add end-to-end signal (human-approval) workflow example"
```

---

## Checkpoint exit criteria

- `river_workflow_signal` table migrates up/down on all three drivers.
- `WorkflowSignalEmit` (idempotent, payload-mismatch-detecting) + `WorkflowSignalList` pass conformance on all three drivers; `make verify/sqlc` clean.
- `waiteval` signal terms evaluate real `payload/attempt/created_at/id/source`.
- `Workflow.Signals().Emit/List/LatestForTask/ListForTask` work end-to-end; `SignalPayloadMismatchError` surfaces.
- The scheduler promotes a signal-gated task after `Emit` — proven by simulation + example.
- All three drivers exercised at the conformance layer (per the CP2 external-review lesson: do NOT leave a driver path untested).

## Hand-off to Checkpoint 4 (Diagnostics)

CP4 adds `Workflow.WaitDiagnostics(ctx, taskName, opts) → WaitDiagnostics{Phase, Summary, ExprResult, Truncated}` (read-only re-evaluation via `waiteval`), `SignalScanLimit`/`Truncated` wiring, and `IncludeAfterResolution` / the per-task signal-resolution view (the `resolved_at` column + `ListForTask` semantics deferred here). No new table.

## Self-review checklist (run after writing)
- Spec §6 table → Task 1; §7 driver methods → Task 2; §3 signal-CEL scope → Task 3; §3 Signals() API → Task 4; §8 scheduler signal load → Task 5; §12 example → Task 6.
- All-three-driver coverage at conformance (Task 2) — the explicit CP2 lesson.
- Type names consistent: `rivertype.WorkflowSignal`, `ErrWorkflowSignalPayloadMismatch`, `WorkflowSignalEmitParams`/`WorkflowSignalListParams`, `waiteval.SignalView`, `WorkflowSignalEmitOpts{IdempotencyKey,Source}`.
- No placeholders; SQL sketches + interfaces are concrete. Idempotency-with-mismatch Go logic is specified.
