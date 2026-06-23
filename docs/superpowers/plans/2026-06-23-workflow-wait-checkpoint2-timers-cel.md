# Workflow Wait-Family — Checkpoint 2 (Timers + CEL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make held wait-bearing tasks actually *resolve*. Build the `waiteval` CEL engine, timer-anchor resolution, and a scheduler pass that — for each pending wait-bearing task whose deps are satisfied — evaluates its `WaitSpec` and promotes it when the expression is true; and that cancels a wait task whose dependency failed (the CP1 hand-off requirement). Signal terms are recognized but resolve to "not yet received" until Checkpoint 3.

**Architecture:** CEL cannot run in SQL, so the leader-elected `WorkflowScheduler` evaluates waits in Go. Each tick it lists pending wait-bearing tasks (`JobList`), loads each task's workflow siblings (`JobGetWorkflowTasks`) to classify dependency state and read dep outputs, then: if a dep failed → cancel the task; if deps incomplete → leave pending; if deps satisfied → evaluate the `WaitSpec` via `waiteval` and, on true, promote. Promotion and cancellation share one new driver method `JobApplyWorkflowWait`.

**Tech Stack:** Go 1.25, `github.com/google/cel-go`, sqlc (Postgres + SQLite), `riverworkflow`, `riverdriver`, `riverdrivertest`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-23-workflow-wait-family-design.md`. CP1 already shipped: `WaitSpec`/`WaitTermSpec`/`TimerSpec` (with json tags), `WorkflowTaskOpts.Wait`, `river:workflow_wait` metadata, and SQL that holds wait-bearing tasks pending.
- **API parity (River Pro):** `WorkflowTimerPollerInterval` config (default 1s); CEL variable scopes — generic `Expr`/`WaitTerm` see `signals`, `timers`, `deps`, `workflow`; signal-term `celExpr` sees `payload, attempt, created_at, id, source`.
- **Three drivers in lockstep.** Postgres SQL edited once in `riverpgxv5/internal/dbsqlc/river_job.sql` (regenerates pgx + databasesql); SQLite in `riversqlite/internal/dbsqlc/river_job.sql`. Regenerate via `make generate/sqlc`; verify `make verify/sqlc` diff-clean.
- **Dep-classification parity:** the Go dep-classifier MUST match `JobUpdateWorkflowReady`'s semantics exactly — a dep counts as satisfied when `completed`, or `cancelled`/`discarded`/missing with the corresponding `IgnoreCancelled/Discarded/DeletedDeps` flag; a dep is a failure when `cancelled`/`discarded`/missing WITHOUT the ignore flag. Honor both workflow-level and task-level (`*bool`) overrides, which CP1 already encodes into metadata keys `river:workflow_ignore_{cancelled,discarded,deleted}_deps`.
- **Determinism:** `waiteval` is pure (no clock/IO); the scheduler passes `now` in. Tests must inject time, never call `time.Now()` inside the engine.
- Metadata keys live in `internal/rivercommon/river_common.go`. Module test roots: `riverworkflow/`, repo root for `riverdriver`/`rivercommon`. Postgres test DSN default `postgres://localhost/river_test?sslmode=disable` works; SQLite in-process.
- DB env confirmed available: Postgres running, `sqlc` v1.31.1 on PATH.

---

### Task 1: Add cel-go and build the `waiteval` evaluation engine

**Files:**
- Create: `riverworkflow/internal/waiteval/waiteval.go`
- Create: `riverworkflow/internal/waiteval/waiteval_test.go`
- Modify: `riverworkflow/go.mod`, `riverworkflow/go.sum` (add `github.com/google/cel-go`)

**Interfaces:**
- Produces (consumed by Tasks 3 and 5):
  - `type TermResult struct { Name string; Value bool }`
  - `type Inputs struct { Timers map[string]bool; Signals map[string]any; Deps map[string]DepView; Workflow map[string]any }`
  - `type DepView struct { Output any; State string }`
  - `type Program struct { /* compiled */ }`
  - `func Compile(terms []TermData, expr string) (*Program, error)` — compiles each term's sub-expression (signal/generic) and the top-level boolean `expr` over the term names; returns an error on any CEL syntax/type error (this is the CEL-syntax validation CP1 deferred).
  - `func (p *Program) Evaluate(in Inputs) (bool, error)` — evaluates terms then `expr`; returns the final boolean.
  - `type TermData struct { Name, Kind, Key, CELExpr string; HasTimer bool }` — the engine's view of a term (decoupled from `riverworkflow.WaitTermSpec` to avoid an import cycle; Task 3 maps between them).
- Kind values mirror CP1: `"signal"`, `"timer"`, `"generic"`.

**Evaluation contract:**
- A **timer** term's value is taken directly from `Inputs.Timers[name]` (the scheduler computes timer fire state in Task 2; the engine does not compute time).
- A **signal** term: if `Inputs.Signals[key]` is absent → term is `false` (not yet received); if present, evaluate the term's `CELExpr` in a sub-environment exposing `payload` (the signal value) and, for CP2, zero-valued `attempt/created_at/id/source` (full signal metadata arrives in CP3). Document with `// PARITY: full signal metadata wired in CP3`.
- A **generic** term: evaluate `CELExpr` in the full environment (`signals`, `timers`, `deps`, `workflow`).
- The top-level `expr` is evaluated in an environment where each term name is a `bool` variable bound to its computed result, PLUS `signals/timers/deps/workflow` for expressions that reference them directly.

- [ ] **Step 1: Add the dependency**

Run: `cd riverworkflow && go get github.com/google/cel-go@latest && go mod tidy`
Expected: `cel-go` appears in `go.mod` require block.

- [ ] **Step 2: Write failing tests**

Create `riverworkflow/internal/waiteval/waiteval_test.go`:

```go
package waiteval

import "testing"

func TestCompileRejectsBadSyntax(t *testing.T) {
	_, err := Compile([]TermData{{Name: "a", Kind: "generic", CELExpr: "1 +"}}, "a")
	if err == nil {
		t.Fatal("expected compile error for bad CEL syntax")
	}
}

func TestTimerTermFromInputs(t *testing.T) {
	p, err := Compile([]TermData{{Name: "deadline", Kind: "timer", HasTimer: true}}, "deadline")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := p.Evaluate(Inputs{Timers: map[string]bool{"deadline": true}})
	if err != nil || !got {
		t.Fatalf("want true,nil; got %v,%v", got, err)
	}
	got, _ = p.Evaluate(Inputs{Timers: map[string]bool{"deadline": false}})
	if got {
		t.Fatal("want false when timer not fired")
	}
}

func TestSignalTermAbsentIsFalse(t *testing.T) {
	p, err := Compile([]TermData{{Name: "ok", Kind: "signal", Key: "approved", CELExpr: "payload.ok"}}, "ok")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := p.Evaluate(Inputs{Signals: map[string]any{}})
	if err != nil || got {
		t.Fatalf("absent signal must be false; got %v,%v", got, err)
	}
	got, err = p.Evaluate(Inputs{Signals: map[string]any{"approved": map[string]any{"ok": true}}})
	if err != nil || !got {
		t.Fatalf("present matching signal must be true; got %v,%v", got, err)
	}
}

func TestGenericTermOverDeps(t *testing.T) {
	p, err := Compile([]TermData{{Name: "big", Kind: "generic", CELExpr: `deps["a"].output.n > 5`}}, "big")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := p.Evaluate(Inputs{Deps: map[string]DepView{"a": {Output: map[string]any{"n": 10.0}}}})
	if err != nil || !got {
		t.Fatalf("want true; got %v,%v", got, err)
	}
}

func TestExprCombinesTerms(t *testing.T) {
	p, err := Compile([]TermData{
		{Name: "t", Kind: "timer", HasTimer: true},
		{Name: "s", Kind: "signal", Key: "k", CELExpr: "payload.ok"},
	}, "t || s")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, _ := p.Evaluate(Inputs{Timers: map[string]bool{"t": true}, Signals: map[string]any{}})
	if !got {
		t.Fatal("t||s with t fired must be true")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd riverworkflow && go test ./internal/waiteval/`
Expected: FAIL — undefined `Compile`/`Inputs`/etc.

- [ ] **Step 4: Implement the engine**

Create `riverworkflow/internal/waiteval/waiteval.go`. Use `cel-go`'s standard API: build a `*cel.Env` with `cel.Variable(...)` declarations for `signals`, `timers`, `deps`, `workflow` (all `cel.MapType(cel.StringType, cel.DynType)` except `timers` which is `MapType(StringType, BoolType)`), compile each term's sub-expression, and compile the top-level `expr` in an env that also declares each term name as `cel.Variable(name, cel.BoolType)`. `Evaluate` builds the activation maps and runs `prg.Eval`. Convert results with `out.Value().(bool)`; a non-bool top-level result is an error. For a signal term, detect absence by checking `in.Signals[term.Key]` presence in Go before evaluating its sub-expression, returning `false` when absent.

Reference cel-go usage (current API):
```go
env, err := cel.NewEnv(
    cel.Variable("signals", cel.MapType(cel.StringType, cel.DynType)),
    cel.Variable("timers", cel.MapType(cel.StringType, cel.BoolType)),
    cel.Variable("deps", cel.MapType(cel.StringType, cel.DynType)),
    cel.Variable("workflow", cel.MapType(cel.StringType, cel.DynType)),
)
ast, iss := env.Compile(expr)
if iss != nil && iss.Err() != nil { return nil, iss.Err() }
prg, err := env.Program(ast)
out, _, err := prg.Eval(map[string]any{"signals": ..., "timers": ..., "deps": ..., "workflow": ...})
b, ok := out.Value().(bool)
```
Keep `waiteval.go` focused on compile+eval; put no scheduler or DB logic here. If the file approaches ~250 lines, split the env-construction helper into the same package but a second file rather than overgrowing one file.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd riverworkflow && go test ./internal/waiteval/ -v`
Expected: PASS (all 5 tests). Output pristine.

- [ ] **Step 6: Commit**

```bash
git add riverworkflow/internal/waiteval/ riverworkflow/go.mod riverworkflow/go.sum
git commit -m "Add waiteval CEL engine (timers, signals-absent, deps, expr)"
```

---

### Task 2: Timer anchor resolution

**Files:**
- Create: `riverworkflow/internal/waiteval/timer.go`
- Create: `riverworkflow/internal/waiteval/timer_test.go`

**Interfaces:**
- Produces (consumed by Task 5):
  - `type TimerAnchors struct { WorkflowCreatedAt time.Time; WaitStartedAt time.Time; DepFinalizedAt map[string]time.Time }`
  - `func ResolveTimer(spec TimerSpecData, anchors TimerAnchors, now time.Time) (fired bool, fireAt time.Time, err error)`
  - `type TimerSpecData struct { Name, Kind string; At time.Time; Dur time.Duration; DepTaskName string }`
- `fireAt` is the absolute instant the timer fires; `fired` is `now >= fireAt`. Kind handling mirrors CP1's `TimerKind*`:
  - `at` → `fireAt = spec.At`
  - `after_wait_started` → `anchors.WaitStartedAt + spec.Dur`
  - `after_workflow_created` → `anchors.WorkflowCreatedAt + spec.Dur`
  - `after_task_finalized` → `anchors.DepFinalizedAt[spec.DepTaskName] + spec.Dur`; if the dep has no finalized time yet, `fired=false` and `fireAt` is the zero time with no error (the dep hasn't finalized, so the timer cannot fire).

- [ ] **Step 1: Write failing tests**

Create `riverworkflow/internal/waiteval/timer_test.go`:

```go
package waiteval

import (
	"testing"
	"time"
)

func TestResolveTimerAt(t *testing.T) {
	base := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	fired, fireAt, err := ResolveTimer(TimerSpecData{Kind: "at", At: base}, TimerAnchors{}, base.Add(time.Second))
	if err != nil || !fired || !fireAt.Equal(base) {
		t.Fatalf("want fired,%v; got %v,%v,%v", base, fired, fireAt, err)
	}
	fired, _, _ = ResolveTimer(TimerSpecData{Kind: "at", At: base}, TimerAnchors{}, base.Add(-time.Second))
	if fired {
		t.Fatal("must not fire before At")
	}
}

func TestResolveTimerAfterWaitStarted(t *testing.T) {
	ws := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	spec := TimerSpecData{Kind: "after_wait_started", Dur: time.Hour}
	fired, fireAt, _ := ResolveTimer(spec, TimerAnchors{WaitStartedAt: ws}, ws.Add(90*time.Minute))
	if !fired || !fireAt.Equal(ws.Add(time.Hour)) {
		t.Fatalf("want fired at +1h; got %v,%v", fired, fireAt)
	}
	fired, _, _ = ResolveTimer(spec, TimerAnchors{WaitStartedAt: ws}, ws.Add(30*time.Minute))
	if fired {
		t.Fatal("must not fire before +1h")
	}
}

func TestResolveTimerAfterTaskFinalizedMissing(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	fired, _, err := ResolveTimer(TimerSpecData{Kind: "after_task_finalized", DepTaskName: "a", Dur: time.Minute}, TimerAnchors{DepFinalizedAt: map[string]time.Time{}}, now)
	if err != nil || fired {
		t.Fatalf("unfinalized dep must not fire and not error; got %v,%v", fired, err)
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cd riverworkflow && go test ./internal/waiteval/ -run TestResolveTimer`
Expected: FAIL — undefined `ResolveTimer`.

- [ ] **Step 3: Implement `timer.go`**

Implement `ResolveTimer` per the Interfaces contract. Pure function; no `time.Now()`. Switch on `spec.Kind`; for an unknown kind return an error. For `after_task_finalized` with a zero/absent `DepFinalizedAt[name]`, return `fired=false, fireAt=time.Time{}, err=nil`.

- [ ] **Step 4: Run to verify pass**

Run: `cd riverworkflow && go test ./internal/waiteval/ -run TestResolveTimer -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/internal/waiteval/timer.go riverworkflow/internal/waiteval/timer_test.go
git commit -m "Add timer anchor resolution to waiteval"
```

---

### Task 3: CEL-syntax validation in WaitSpec.Validate + spec serialization mapping

**Files:**
- Modify: `riverworkflow/wait.go` (`Validate`, add a `toEngineTerms` mapper + a `serializableWait`/JSON round-trip helper)
- Modify: `riverworkflow/wait_test.go`

**Interfaces:**
- Consumes: `waiteval.Compile`, `waiteval.TermData` (Task 1).
- Produces (consumed by Task 5's scheduler):
  - `func (s *WaitSpec) toEngineTerms() []waiteval.TermData` — maps `WaitTermSpec` → `waiteval.TermData` (Kind/Key/CELExpr; `HasTimer = (t.Timer != nil)`).
  - `func parseWaitSpec(raw []byte) (*WaitSpec, error)` — unmarshal the metadata JSON back into a `WaitSpec` (round-trips CP1's json tags).
- `Validate()` now ALSO calls `waiteval.Compile(s.toEngineTerms(), s.Expr)` and returns its error, replacing the CP1 `// PARITY: CEL-syntax check pending` deferral. Remove that PARITY comment.

- [ ] **Step 1: Write failing tests**

Append to `riverworkflow/wait_test.go`:

```go
func TestWaitSpecValidateRejectsBadCEL(t *testing.T) {
	s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("a", "1 +")}, Expr: "a"}
	if err := s.Validate(); err == nil {
		t.Fatal("expected CEL syntax error from Validate")
	}
}

func TestWaitSpecValidateRejectsUnknownTermInExpr(t *testing.T) {
	s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("a", "true")}, Expr: "a && ghost"}
	if err := s.Validate(); err == nil {
		t.Fatal("expected error for undefined term 'ghost' in Expr")
	}
}

func TestParseWaitSpecRoundTrip(t *testing.T) {
	orig := &WaitSpec{
		Terms: []WaitTermSpec{WaitTermSignal("ok", "approved", "payload.ok")},
		Expr:  "ok",
	}
	raw, err := json.Marshal(orig)
	require.NoError(t, err)
	got, err := parseWaitSpec(raw)
	require.NoError(t, err)
	require.Equal(t, orig.Expr, got.Expr)
	require.Len(t, got.Terms, 1)
	require.Equal(t, "approved", got.Terms[0].Key)
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cd riverworkflow && go test ./... -run 'TestWaitSpecValidateRejects|TestParseWaitSpecRoundTrip'`
Expected: FAIL — `toEngineTerms`/`parseWaitSpec` undefined; bad-CEL test fails because Validate doesn't yet compile.

- [ ] **Step 3: Implement**

In `wait.go`: add `toEngineTerms()` and `parseWaitSpec()`; extend `Validate()` to compile via `waiteval`. Note compiling the top-level `Expr` against term-name bool variables is what catches `ghost` (an undefined variable) — ensure `waiteval.Compile` declares ONLY the known term names plus the four scope maps, so an unknown identifier is a compile error.

- [ ] **Step 4: Run to verify pass + full package**

Run: `cd riverworkflow && go test ./...`
Expected: PASS (existing CP1 tests still green — note `WaitSpec.Validate` is now stricter; confirm CP1's `TestWorkflowWaitMetadata` spec `{Expr:"ok", Terms:[signal ok]}` still validates).

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/wait.go riverworkflow/wait_test.go
git commit -m "Validate CEL syntax in WaitSpec.Validate and add spec round-trip"
```

---

### Task 4: Driver method `JobApplyWorkflowWait` (promote | cancel) across all drivers

**Files:**
- Modify: `riverdriver/river_driver_interface.go` (method + `JobApplyWorkflowWaitParams`)
- Modify: `riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql`, `riverdriver/riversqlite/internal/dbsqlc/river_job.sql`
- Regenerate: all three `river_job.sql.go`
- Modify: `riverdriver/riverpgxv5/river_pgx_v5_driver.go`, `riverdriver/riverdatabasesql/river_database_sql_driver.go`, `riverdriver/riversqlite/river_sqlite_driver.go`
- Test: `riverdriver/riverdrivertest/job_workflow.go` + register in `riverdrivertest.go`

**Interfaces:**
- Produces (consumed by Task 5):
  - `JobApplyWorkflowWait(ctx, *JobApplyWorkflowWaitParams) (*rivertype.JobRow, error)`
  - `type JobApplyWorkflowWaitParams struct { ID int64; Outcome string; Now time.Time; Schema string }` — `Outcome` ∈ `"promote"` | `"cancel"`.
- Semantics (operate only on a row still in `pending`; a row no longer pending returns unchanged / no-op):
  - `promote`: `state` → `scheduled` if `scheduled_at > now`, else `available`; set metadata `river:workflow_wait_resolved_at = now` (RFC3339).
  - `cancel`: `state` → `cancelled`, `finalized_at = now`; set metadata `river:workflow_wait_failed_reason = "dependency failed"`.

**Metadata keys to add** (`internal/rivercommon/river_common.go`): `MetadataKeyWorkflowWaitResolvedAt = "river:workflow_wait_resolved_at"`, `MetadataKeyWorkflowWaitFailedReason = "river:workflow_wait_failed_reason"`.

- [ ] **Step 1: Write the failing conformance exercise**

In `riverdriver/riverdrivertest/job_workflow.go`, add `exerciseJobApplyWorkflowWait` following the `exerciseJobUpdateWorkflowReady` pattern. It must insert a pending wait-bearing task (use the existing `insertWorkflowJob` with the `Wait` field added in CP1) and assert:
- `promote` with `scheduled_at` in the past → state `available`, metadata has `river:workflow_wait_resolved_at`.
- `promote` with `scheduled_at` in the future → state `scheduled`.
- `cancel` → state `cancelled`, `finalized_at` set, metadata has `river:workflow_wait_failed_reason`.
Use `exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id})` to read back. Register it in `Exercise()` next to `exerciseJobUpdateWorkflowReady`.

- [ ] **Step 2: Run against pgx to verify fail**

Run: `go test ./riverdriver/riverdrivertest/ -run 'TestDriverRiverPgxV5/.*/JobApplyWorkflowWait' -v`
Expected: FAIL — method undefined / not registered.

- [ ] **Step 3: Add the metadata constants**

Add the two constants above to `internal/rivercommon/river_common.go` in the workflow const block.

- [ ] **Step 4: Add the SQL (Postgres + SQLite)**

In `riverpgxv5/internal/dbsqlc/river_job.sql` add `JobApplyWorkflowWait` (`:one`). Postgres sketch:
```sql
-- name: JobApplyWorkflowWait :one
UPDATE /* TEMPLATE: schema */river_job
SET
  state = CASE
    WHEN @outcome::text = 'cancel' THEN 'cancelled'::/* TEMPLATE: schema */river_job_state
    WHEN scheduled_at > @now::timestamptz THEN 'scheduled'::/* TEMPLATE: schema */river_job_state
    ELSE 'available'::/* TEMPLATE: schema */river_job_state
  END,
  finalized_at = CASE WHEN @outcome::text = 'cancel' THEN @now::timestamptz ELSE finalized_at END,
  metadata = CASE
    WHEN @outcome::text = 'cancel'
      THEN jsonb_set(metadata, '{river:workflow_wait_failed_reason}', to_jsonb('dependency failed'::text), true)
    ELSE jsonb_set(metadata, '{river:workflow_wait_resolved_at}', to_jsonb(@now::timestamptz), true)
  END
WHERE id = @id::bigint
  AND state = 'pending'
RETURNING *;
```
In `riversqlite/internal/dbsqlc/river_job.sql` add the SQLite equivalent using `json_set(metadata, '$."river:workflow_wait_resolved_at"', ...)`, `datetime(@now)` comparisons, and `cast` to the text state as the existing sqlite workflow queries do. Match the dialect conventions already in that file.

- [ ] **Step 5: Regenerate + implement the three driver wrappers**

Run: `make generate/sqlc` then `make verify/sqlc` (diff-clean). Add the `JobApplyWorkflowWait` wrapper to each driver's main file, following the existing `JobGetWorkflowTasks` wrapper pattern (build the dbsqlc params, call the generated function, map the row via `jobRowFromInternal`/`sliceutil`). Add the method + params struct to `riverdriver/river_driver_interface.go`.

- [ ] **Step 6: Run conformance on all three drivers**

Run:
```bash
go test ./riverdriver/riverdrivertest/ -run 'TestDriverRiverPgxV5/.*/JobApplyWorkflowWait' -v
go test ./riverdriver/riverdrivertest/ -run 'TestDriverRiverDatabaseSQLLibPQ/.*/JobApplyWorkflowWait' -v
go test ./riverdriver/riverdrivertest/ -run 'TestDriverRiverSQLiteModernC/JobApplyWorkflowWait' -v
```
Expected: PASS (ignore the unrelated pre-existing `QueueNameList`/`JobKindList` databasesql failures).

- [ ] **Step 7: Commit**

```bash
git add riverdriver/ internal/rivercommon/river_common.go
git commit -m "Add JobApplyWorkflowWait (promote|cancel) driver method across drivers"
```

---

### Task 5: Scheduler wait-evaluation pass

**Files:**
- Modify: `riverworkflow/internal/workflowscheduler/workflow_scheduler.go` (add `evaluateWaits`, call from `runOnce`; add `TimerPollerInterval` to `Config`; tighten tick interval)
- Create: `riverworkflow/internal/workflowscheduler/wait_classify.go` (Go dep-classifier, pure, unit-tested)
- Modify: `riverworkflow/config.go` (`WorkflowTimerPollerInterval`), `riverworkflow/client.go` (thread it into `workflowscheduler.Config`), `riverworkflow/driver_plugin.go` if the pilot carries config
- Test: `riverworkflow/internal/workflowscheduler/wait_classify_test.go` (unit), and extend `riverworkflow/simulation_test.go` (integration)

**Interfaces:**
- Consumes: `waiteval.Compile/Evaluate/ResolveTimer/TimerAnchors`, `JobApplyWorkflowWait`, `JobList`, `JobGetWorkflowTasks`, `parseWaitSpec` semantics (re-implement spec parsing inside the scheduler package against the metadata JSON, OR expose a small parser — see note).
- Produces: `evaluateWaits(ctx) error` on `WorkflowScheduler`; `classifyDeps(task, siblings) (DepStatus, ...)` in `wait_classify.go` with `DepStatus ∈ {Satisfied, Pending, Failed}`.

**Dep classifier (`wait_classify.go`) — pure function, mirrors `JobUpdateWorkflowReady`:**
Input: the candidate task's metadata (deps list + ignore flags) and the workflow's sibling task rows (name → state, finalized_at, metadata.output). Output:
- `Failed` if any dep is `cancelled`/`discarded`/missing WITHOUT the matching ignore flag;
- else `Pending` if any dep is not yet in a terminal state (`completed`/`cancelled`/`discarded`) — i.e. still `available/running/retryable/scheduled/pending`;
- else `Satisfied`.
Unit-test every branch.

**`evaluateWaits` flow (per tick, batched):**
1. `JobList` with `WhereClause: "state = 'pending' AND metadata ? 'river:workflow_wait'"` (SQLite: `json_extract(...) IS NOT NULL` — JobList where-clauses are driver-portable strings; mirror the dialect handling `cancelExpiredWorkflows` uses, or add a small dialect note. If a single portable clause is infeasible, gate by what `cancelExpiredWorkflows` already does — it uses `metadata ? '...'`, so Postgres is fine; for SQLite add the `json_extract` form).
2. For each task: parse workflow_id + deps + ignore flags + WaitSpec from metadata. `JobGetWorkflowTasks(workflowID)` → siblings.
3. `classifyDeps`:
   - `Failed` → `JobApplyWorkflowWait{Outcome:"cancel"}`.
   - `Pending` → skip.
   - `Satisfied` → build `waiteval.Inputs`: timers via `ResolveTimer` over each timer term using anchors (workflow_created from workflow-id ULID timestamp per spec §5; wait_started recorded on first evaluation into metadata `river:workflow_wait_started_at`; dep finalized_at from sibling rows); deps map from siblings' `output` metadata; signals empty (CP3). Compile (cache by spec-hash) + `Evaluate`. On `true` → `JobApplyWorkflowWait{Outcome:"promote"}`.
4. Record `river:workflow_wait_started_at` via `JobUpdate{MetadataDoMerge:true}` on first sight (so timer anchors are stable). Add metadata key `MetadataKeyWorkflowWaitStartedAt = "river:workflow_wait_started_at"`.

**Config/tick:** add `Config.TimerPollerInterval`; when any pending wait task carries a timer, the loop ticks at `min(Interval, TimerPollerInterval)`. Simplest correct implementation: run `evaluateWaits` every tick and set the ticker to `min(Interval, TimerPollerInterval)` whenever `TimerPollerInterval > 0`. `riverworkflow.Config.WorkflowTimerPollerInterval` defaults to 1s (set in `applyDefaults`).

- [ ] **Step 1: Write failing unit tests for `classifyDeps`**

Create `wait_classify_test.go` covering: all-completed → Satisfied; one running → Pending; discarded-without-ignore → Failed; discarded-with-ignore → Satisfied; missing-without-ignore-deleted → Failed; missing-with-ignore-deleted → Satisfied.

- [ ] **Step 2: Run to verify fail**, then **Step 3: implement `wait_classify.go`**, **Step 4: tests pass.**

Run: `cd riverworkflow && go test ./internal/workflowscheduler/ -run TestClassifyDeps -v`

- [ ] **Step 5: Write a failing integration test in `simulation_test.go`**

Add a test that builds a real workflow (using the existing simulation harness in `riverworkflow/simulation_test.go`) with: (a) a task gated by `WaitTermTimer(TimerAfterWaitStarted("d", <short>))` that becomes available after the scheduler ticks past the duration; (b) a task whose dep is discarded and `Wait` set, asserting it is cancelled. Drive the scheduler with injected time. Follow the existing simulation test's setup for constructing a client + advancing the scheduler.

- [ ] **Step 6: Implement `evaluateWaits` + config wiring**, then run the integration test to GREEN.

Run: `cd riverworkflow && go test ./... -run 'TestWorkflow.*Wait|Simulation'`
Expected: PASS.

- [ ] **Step 7: Full package + scheduler package tests**

Run: `cd riverworkflow && go test ./...`
Expected: PASS, pristine.

- [ ] **Step 8: Commit**

```bash
git add riverworkflow/internal/workflowscheduler/ riverworkflow/config.go riverworkflow/client.go riverworkflow/driver_plugin.go internal/rivercommon/river_common.go
git commit -m "Add scheduler wait-evaluation pass: resolve timers, cancel failed-dep waits, promote"
```

---

### Task 6: End-to-end timer example

**Files:**
- Create: `riverworkflow/example_workflow_wait_test.go`

**Interfaces:** Consumes the public API only.

- [ ] **Step 1: Write an `Example` test** demonstrating a two-task workflow where the second task waits on `WaitTermTimer(TimerAfterWaitStarted(...))`, mirroring the style of the existing `example_workflow_test.go`. Use `// Output:` so `go test` runs it.

- [ ] **Step 2: Run it**

Run: `cd riverworkflow && go test ./... -run Example`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add riverworkflow/example_workflow_wait_test.go
git commit -m "Add end-to-end timer-gated workflow example"
```

---

## Checkpoint exit criteria

- `waiteval` compiles/evaluates CEL over timers, (absent) signals, deps, and combined exprs; pure and unit-tested.
- Timer anchors resolve correctly for all four `TimerKind`s.
- `WaitSpec.Validate()` now rejects malformed CEL and unknown term references (CP1 deferral closed).
- `JobApplyWorkflowWait` promotes/cancels a single pending wait task across all three drivers (conformance green); `make verify/sqlc` clean.
- The scheduler resolves timer-gated waits → promotes, and cancels waits whose deps failed (closing the CP1 hand-off requirement) — proven by simulation tests.
- A timer-gated task goes available end-to-end in the example.

## Hand-off to Checkpoint 3 (Signals)

Signal terms currently always resolve `false` (absent). CP3 adds the `river_workflow_signal` table + driver methods + `Workflow.Signals()` API, and wires real signal values + metadata (`payload/attempt/created_at/id/source`) into `waiteval.Inputs.Signals`, replacing the `// PARITY: full signal metadata wired in CP3` stub. The scheduler's `evaluateWaits` will additionally load signals for each task before `Evaluate`.

## Self-review checklist (run after writing the plan)

- Spec §4 (waiteval) → Task 1; §5 (timers) → Task 2; §3 Validate CEL → Task 3; §7 driver (promote/cancel) → Task 4; §8 scheduler + §2 dep-failure → Task 5; §12 example → Task 6. All covered.
- No placeholders: every code step shows code or an exact command. Scheduler logic (Task 5) is specified by contract + the existing SQL it must mirror; the implementer writes the port — flagged as a judgment task for a capable model.
- Type consistency: `waiteval.TermData`/`Inputs`/`DepView`/`TimerSpecData`/`TimerAnchors`, `JobApplyWorkflowWaitParams{ID,Outcome,Now,Schema}`, metadata keys `river:workflow_wait_{resolved_at,failed_reason,started_at}` are used identically across tasks.
