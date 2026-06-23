# Design: River Pro wait-family parity for `riverworkflow`

**Date:** 2026-06-23
**Status:** Approved (design); implementation pending
**Module:** `riverworkflow` (+ `riverdriver`, `internal/rivercommon`)
**Goal:** Add River Pro's full workflow "wait" feature family — **signals**, **CEL wait expressions**, **timer-based waits**, and **wait diagnostics** — to the `techpartners-asia/river` fork, with exact River Pro API parity, across all three drivers, with signals in a dedicated table.

---

## 1. Background & scope

### 1.1 What exists today

The fork's `riverworkflow` package implements the **DAG core** of River Pro workflows:

- `Workflow.Add(name, args, jobOpts, *WorkflowTaskOpts{Deps})` — fan-out / fan-in DAG
- `Prepare` / `PrepareTx` + `validate()` — cycle / unknown-dep / duplicate / empty checks
- `IgnoreCancelled/Discarded/DeletedDeps` (workflow- and task-level)
- `WorkflowFromExisting` — dynamic workflows
- `WorkflowCancel`, `WorkflowRetry` (modes: `failed_only` / `failed_and_downstream` / `all`)
- `LoadAll` / `LoadDeps` (recursive) / `WorkflowTasks.Output` — dependency output loading
- `WorkflowOpts.DeadlineAt` — workflow deadline cancellation
- Leader-elected `WorkflowScheduler` service that promotes pending tasks

**Readiness is decided in SQL.** `Executor.JobUpdateWorkflowReady` promotes a pending task to `available`/`scheduled` once its declared deps reach terminal states. The scheduler (`workflowscheduler.runOnce`) calls it each tick (default 5s) plus `cancelExpiredWorkflows` for deadlines.

### 1.2 What is missing (this design adds)

| Pro feature | API (verbatim from public docs) |
|---|---|
| **Signals** | `Workflow.Signals().Emit/LatestForTask/ListForTask/List`; `WorkflowSignalEmitOpts{IdempotencyKey, Source}`; `SignalPayloadMismatchError` |
| **CEL waits** | `WaitSpec{Terms, Expr}`; `WaitTermSignal/WaitTermTimer/WaitTerm`; `.Label()`; `WaitSpec.Validate()` |
| **Timer waits** | `TimerAt`, `TimerAfterWaitStarted`, `TimerAfterWorkflowCreated`, `TimerAfterTaskFinalized`; `WorkflowTimerPollerInterval` |
| **Diagnostics** | `Workflow.WaitDiagnostics` → `WaitDiagnostics{Phase, Summary, ExprResult, Truncated}`; `SignalScanLimit` |
| Attachment | `WorkflowTaskOpts.Wait *WaitSpec` |

### 1.3 Decisions (locked with user)

- **API fidelity:** exact River Pro parity (drop-in portable).
- **Driver scope:** all three — `riverpgxv5`, `riverdatabasesql`, `riversqlite` — with full `riverdrivertest` conformance.
- **Signal storage:** dedicated `river_workflow_signal` table (required for idempotency keys, audit list views, scan limits).

---

## 2. The architectural pivot

CEL expressions cannot be evaluated in SQL. Therefore readiness evaluation **splits by whether a task carries a `Wait`**:

- **No `Wait`** → unchanged. The existing set-based SQL promotion (`JobUpdateWorkflowReady`) handles it. The common case must not regress.
- **Has `Wait`** → the SQL promotion path is taught to **skip** the task even when its deps complete. The **Go scheduler** evaluates the task's CEL `WaitSpec` each tick and promotes it when the expression resolves `true`.

**Promotion rule for a wait-bearing task:** promote only when **(deps satisfied) AND (wait resolved)**.

Deps remain the existing pending-until-deps-terminal mechanism. The wait is an *additional* gate layered on top. A task with deps **and** a wait stays pending until both are satisfied; the deps gate is still evaluated cheaply (the scheduler only considers wait-bearing tasks whose deps are already satisfied).

### 2.1 Evaluation flow (scheduler, per tick)

1. Fetch wait-bearing, deps-satisfied, still-pending tasks (`JobGetWorkflowWaitTasks`), including the rows of their deps (for `deps["t"].output`).
2. For each task, resolve term inputs:
   - **signal terms** → query `river_workflow_signal` (bounded by `SignalScanLimit`)
   - **timer terms** → compute fire time from the anchor (no storage; see §5)
   - **dep outputs** → read `output` from each dep job row's metadata
3. Build the CEL environment (`signals`, `timers`, `deps`, `workflow`), compile + evaluate `Expr`.
4. `true` → `JobPromoteWorkflowTask(id)` (set `available`/`scheduled`, write resolution evidence to metadata). `false` → leave pending.

### 2.2 Tick latency

Timer terms resolve at wall-clock times. The scheduler honors `WorkflowTimerPollerInterval` (Pro default 1s) so a timer fires with at most ~interval latency. The scheduler computes the earliest pending timer fire time and need not tick faster than the soonest deadline; the configured interval is the floor.

---

## 3. Public API (package `riverworkflow`)

All names mirror River Pro verbatim.

```go
// Attachment — extends the existing struct.
type WorkflowTaskOpts struct {
    Deps                []string
    IgnoreCancelledDeps *bool
    IgnoreDiscardedDeps *bool
    IgnoreDeletedDeps   *bool
    Wait                *WaitSpec // NEW
}

// Wait specification.
type WaitSpec struct {
    Terms []WaitTermSpec
    Expr  string // CEL boolean expression combining term names
}
func (s *WaitSpec) Validate() error // term-name uniqueness, CEL syntax, dep refs, timer anchors

type WaitTermSpec struct { /* name, kind, key, celExpr, timer */ }
func (t WaitTermSpec) Label(string) WaitTermSpec

func WaitTermSignal(name, key, celExpr string) WaitTermSpec
func WaitTermTimer(spec TimerSpec) WaitTermSpec
func WaitTerm(name, celExpr string) WaitTermSpec

// Timer builders.
type TimerSpec struct { /* name, kind, at, dur, depTaskName */ }
func TimerAt(name string, t time.Time) TimerSpec
func TimerAfterWaitStarted(name string, d time.Duration) TimerSpec
func TimerAfterWorkflowCreated(name string, d time.Duration) TimerSpec
func TimerAfterTaskFinalized(name, depTaskName string, d time.Duration) TimerSpec

// Signals.
func (w *Workflow[TTx]) Signals() *WorkflowSignals
type WorkflowSignals struct { /* bound to workflow id + exec */ }
func (s *WorkflowSignals) Emit(ctx, key string, payload any, opts *WorkflowSignalEmitOpts) error
func (s *WorkflowSignals) LatestForTask(ctx, taskName, key string, opts *WorkflowSignalLatestForTaskOpts) (*Signal, error)
func (s *WorkflowSignals) ListForTask(ctx, taskName, key string, params *WorkflowSignalListForTaskParams) ([]*Signal, error)
func (s *WorkflowSignals) List(ctx, params *WorkflowSignalListParams) ([]*Signal, error)
// + Tx variants for Emit.

type WorkflowSignalEmitOpts struct { IdempotencyKey string; Source string }
var ErrSignalPayloadMismatch = ... // SignalPayloadMismatchError on idempotency-key reuse w/ different payload

// Diagnostics.
func (w *Workflow[TTx]) WaitDiagnostics(ctx, taskName string, opts *WorkflowWaitDiagnosticsOpts) (*WaitDiagnostics, error)
type WaitDiagnostics struct { Phase WaitPhase; Summary string; ExprResult bool; Truncated bool }
const ( WaitPhasePending WaitPhase = ...; WaitPhaseResolved ... )
```

**Config** (on `riverworkflow.Config`):

```go
type Config struct {
    river.Config
    WorkflowScheduler           WorkflowSchedulerConfig
    WorkflowTimerPollerInterval time.Duration // NEW, default 1s
    SignalScanLimit             int           // NEW, default 10_000, cap 100_000
}
```

**CEL variable scopes:**
- Signal-term `celExpr`: `payload`, `attempt`, `created_at`, `id`, `source`
- Generic `Expr` / `WaitTerm`: `signals["key"]`, `timers["name"]`, `deps["task"].output`, `workflow`

`WaitSpec.Validate()` is invoked inside `Workflow.Prepare`/`PrepareTx`; the `Wait` (as JSON) is injected into task metadata during `renderTaskOpts`, and `opts.Pending = true` is forced for wait-bearing tasks (as it already is for tasks with deps).

---

## 4. CEL engine — `riverworkflow/internal/waiteval`

A focused, side-effect-free package wrapping `github.com/google/cel-go`.

Responsibilities:
- Build a CEL environment declaring `signals` (map), `timers` (map), `deps` (map of objects with `output`), `workflow` (object).
- Compile per-signal-term sub-expressions in their restricted scope (`payload, attempt, created_at, id, source`).
- Compile and evaluate the top-level `Expr` over the named term results.
- Return `(result bool, exprText string, err error)` for both the scheduler (promotion) and diagnostics (read-only).

Interface sketch:

```go
type Program struct { /* compiled cel programs */ }
func Compile(spec WaitSpecData) (*Program, error)      // used by Validate() and scheduler
func (p *Program) Evaluate(in Inputs) (Result, error)  // pure
```

New dependency: `github.com/google/cel-go` added to `riverworkflow/go.mod`. (Note: pulls in `cel-go` + protobuf; acceptable, documented.)

---

## 5. Timers (no storage)

Timer terms are computed from an **anchor + duration/instant**; no rows required.

| Builder | Fire time |
|---|---|
| `TimerAt(t)` | `t` (absolute) |
| `TimerAfterWaitStarted(d)` | wait-start time + `d` |
| `TimerAfterWorkflowCreated(d)` | workflow created_at + `d` |
| `TimerAfterTaskFinalized(dep, d)` | dep's `finalized_at` + `d` |

Anchors:
- **Workflow created_at** — decoded from the workflow id ULID's timestamp component (stable, available without a DB read; the id is already in task metadata).
- **Wait-started** — when the task's deps became satisfied and it entered wait evaluation. Recorded once into metadata (`river:workflow_wait_started_at`) on first scheduler evaluation, so it is stable across ticks.
- **Task finalized** — `finalized_at` from the dep job row (already loaded for dep outputs).

A timer term's value in CEL is whether `now >= fireTime`. The scheduler uses the minimum unfired timer across pending tasks to bound its sleep, floored by `WorkflowTimerPollerInterval`.

---

## 6. Signal storage

### 6.1 Table `river_workflow_signal`

```
id              <driver bigserial/integer pk>
workflow_id     text       not null
signal_key      text       not null
payload         jsonb      not null      (sqlite: text/json)
idempotency_key text       null
source          text       null
created_at      timestamptz not null default now()
resolved_at     timestamptz null         (set when a waiting task consumed it; supports IncludeAfterResolution)
```

Constraints / indexes:
- `UNIQUE (workflow_id, idempotency_key)` where `idempotency_key` is not null → idempotency. Re-emit with same key + same payload is a no-op; same key + different payload → `ErrSignalPayloadMismatch`.
- Index `(workflow_id, signal_key, created_at)` for `LatestForTask` / `ListForTask` / scheduler scans.

Migration `009_workflow_signals.{up,down}.sql` in each driver's `migration/main/`, dialect-adjusted (`timestamptz`/`now()`/`jsonb` for Postgres; `timestamp`/`CURRENT_TIMESTAMP`/`text` for SQLite), following the `008_durable_periodic_jobs` precedent (`/* TEMPLATE: schema */` prefix on table names).

### 6.2 Signal scoping vs. tasks

Signals are **workflow-scoped** (`workflow_id` + `signal_key`). The per-task read APIs (`LatestForTask(taskName, key)`) filter the workflow's signals by key and apply the task's wait-start time / resolution status; `taskName` selects which task's wait context is used for `IncludeAfterResolution`, not a storage partition.

---

## 7. Driver surface

### 7.1 New `Executor` methods (interface in `riverdriver/river_driver_interface.go`)

| Method | Purpose |
|---|---|
| `WorkflowSignalEmit(ctx, *WorkflowSignalEmitParams) (*Signal, error)` | Insert signal; enforce idempotency; detect payload mismatch. |
| `WorkflowSignalLatestForTask(ctx, *...Params) (*Signal, error)` | Newest signal for `(workflow, key)`. |
| `WorkflowSignalListForTask(ctx, *...Params) ([]*Signal, error)` | Bounded list (scan limit). |
| `WorkflowSignalList(ctx, *...Params) ([]*Signal, error)` | Workflow-wide audit list. |
| `JobGetWorkflowWaitTasks(ctx, *...Params) ([]*WorkflowWaitTask, error)` | Pending + deps-satisfied + carries `river:workflow_wait`; returns task + dep rows. |
| `JobPromoteWorkflowTask(ctx, *...Params) (*rivertype.JobRow, error)` | Promote one resolved task; write resolution evidence. |

### 7.2 Modified method

`JobUpdateWorkflowReady` (SQL, all three drivers): add `AND metadata->'river:workflow_wait' IS NULL` (dialect-appropriate) so wait-bearing tasks are **never** promoted by the SQL fast path. **Risk: must not regress non-wait promotion** — covered by existing conformance tests plus a new "skips wait-bearing tasks" case.

### 7.3 Per driver

For each new/changed method: SQL in `internal/dbsqlc/river_job.sql` (+ a new `river_workflow_signal.sql` for signal queries), regenerate `*.sql.go` via sqlc, implement the wrapper in the driver's main file (`river_pgx_v5_driver.go`, `river_database_sql_driver.go`, `river_sqlite_driver.go`), following the existing `JobGetWorkflowTasks` pattern. Param structs live in `river_driver_interface.go`.

### 7.4 Conformance

Add exercises to `riverdriver/riverdrivertest/` (new `workflow_signal.go`, extend `job_workflow.go`) and register them in `Exercise()`:
- signal emit + idempotency (same key/same payload no-op; same key/different payload → mismatch)
- latest/list/listForTask ordering and scan-limit truncation
- `JobGetWorkflowWaitTasks` selection (only pending + deps-satisfied + wait-bearing)
- `JobPromoteWorkflowTask`
- `JobUpdateWorkflowReady` skips wait-bearing tasks

---

## 8. Scheduler changes — `workflowscheduler`

`runOnce` gains a **wait-evaluation pass** after the existing dep-promotion + deadline passes:

1. `JobGetWorkflowWaitTasks` (batched).
2. For each: load/compile its `WaitSpec` (cache by spec hash), resolve inputs (§2.1), evaluate.
3. Record `river:workflow_wait_started_at` on first sight (timer anchor stability).
4. On `true` → `JobPromoteWorkflowTask`. On `false` → leave; optionally persist a lightweight diagnostics snapshot.

Tick interval: `min(WorkflowScheduler.Interval, WorkflowTimerPollerInterval)` when any timer waits are pending, else the existing interval.

---

## 9. Metadata keys (`internal/rivercommon`)

```go
MetadataKeyWorkflowWait          = "river:workflow_wait"            // WaitSpec JSON
MetadataKeyWorkflowWaitStartedAt = "river:workflow_wait_started_at" // RFC3339, timer anchor
MetadataKeyWorkflowWaitResolved  = "river:workflow_wait_resolved_at"// evidence
```

---

## 10. Diagnostics

`WaitDiagnostics` reuses the `waiteval` engine in **read-only** mode (no evidence write, no promotion). It resolves current inputs, evaluates `Expr`, and reports:

- `Phase` — `WaitPhasePending` / `WaitPhaseResolved`
- `Summary` — human-readable per-term status
- `ExprResult` — current boolean
- `Truncated` — true when signal rows scanned hit `SignalScanLimit`

`SignalScanLimit` default 10_000, capped 100_000.

---

## 11. Build sequence

One design (this doc); implement in four checkpoints, each compiling with green tests before the next:

1. **Foundation** — metadata key; `WaitSpec`/`WaitTermSpec`/`TimerSpec` types; `Validate()`; `WorkflowTaskOpts.Wait`; metadata injection in `renderTaskOpts`; SQL `JobUpdateWorkflowReady` skip-wait across 3 drivers; scheduler no-op wait pass. **Proves wait-bearing tasks are held.**
2. **Timers + CEL** — `waiteval` engine (`cel-go`); timer builders + anchor math; scheduler resolves timer-only waits → promote. (No storage.)
3. **Signals** — migration `009` × 3 drivers; signal driver methods + conformance; `Signals()` API; signal terms wired into CEL.
4. **Diagnostics** — `WaitDiagnostics`, `SignalScanLimit`/`Truncated`, `IncludeAfterResolution`, audit list views.

---

## 12. Testing

- **Unit:** `waiteval` CEL evaluation (signal/timer/dep/combined exprs); timer-anchor computation; `WaitSpec.Validate` rejects bad term names / CEL syntax / unknown dep refs / bad timer anchors.
- **Conformance** (`riverdrivertest`, all 3 drivers): signal CRUD + idempotency + scan limit; wait-task selection; promotion; SQL skip.
- **Simulation** (`riverworkflow/simulation_test.go`): signal-gated task end-to-end; timer-gated task; CEL combining signal + timer + dep output; diagnostics snapshot.
- **E2E / examples** (`example_workflow_test.go`): a human-approval (signal) gate and a delayed (timer) step.

---

## 13. Known limitations / risks

1. **Pro is closed-source.** The public **API surface** is mirrored verbatim, but some **internal behavioral semantics** — exact `IncludeAfterResolution` ordering, evidence/diagnostics serialization format, precise `Truncated` thresholds — are **inferred from public docs, not verified against Pro's implementation**. Build targets the documented contract; spots where behavior is guessed will be marked `// PARITY: inferred` in code. Byte-level parity with Pro internals is not guaranteed.
2. **Wait-bearing tasks bypass the set-based SQL promotion** and are evaluated per-task in the leader's Go scheduler. Fine at typical workflow scale; document the cost. Mitigations: spec-hash program cache, batched fetch, scan limits.
3. **Modifying `JobUpdateWorkflowReady` across three drivers** is the highest-regression-risk change; gated by existing + new conformance tests.
4. **`cel-go` dependency** enlarges the `riverworkflow` module's dependency graph (protobuf). Accepted.
5. **SQLite** JSON / `jsonb` differences require care in the signal table and the metadata skip predicate; conformance covers all three dialects.

---

## 14. Out of scope

- River UI workflow visualization (Pro-only, not in OSS UI).
- Any change to non-workflow job behavior.
- Migration of the existing `settlement` pipeline onto workflows (separate decision; this only adds capability).
