# Workflow Wait-Family — Checkpoint 1 (Foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a workflow task able to carry a `Wait` spec that holds it `pending` — the SQL fast-path promotion must never advance a wait-bearing task even when its deps complete. No CEL evaluation yet; this checkpoint only proves wait-bearing tasks are *held*.

**Architecture:** A `WaitSpec` is attached via `WorkflowTaskOpts.Wait`, serialized into job metadata under `river:workflow_wait` at `Prepare` time, and forces the task `pending`. The dep-promotion SQL (`JobUpdateWorkflowReady` for pgx/databasesql; `JobClassifyWorkflowReady` for sqlite) gains a predicate that excludes any task carrying that metadata key, so wait-bearing tasks fall through to the Go scheduler (added in Checkpoint 2).

**Tech Stack:** Go, sqlc (Postgres + SQLite), `riverworkflow`, `riverdriver`, `riverdrivertest`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-23-workflow-wait-family-design.md`. Every task implicitly inherits it.
- **API parity:** public names mirror River Pro verbatim (`WaitSpec`, `WaitTermSpec`, `TimerSpec`, `WaitTermSignal/WaitTermTimer/WaitTerm`, `Timer*` builders, `WorkflowTaskOpts.Wait`).
- **Three drivers** stay in lockstep. `riverdatabasesql` has **no** `.sql` sources — it regenerates from `riverpgxv5/internal/dbsqlc/*.sql`. So Postgres SQL is edited **once** in `riverpgxv5`; SQLite SQL is edited in `riversqlite`.
- **No new dependencies this checkpoint.** `cel-go` arrives in Checkpoint 2; `WaitSpec.Validate()` here does **structural** checks only (CEL-syntax validation is deferred and marked).
- Regenerate sqlc with `make generate/sqlc`; verify with `make verify/sqlc`.
- Metadata key prefix convention: `river:workflow_*` (see existing keys in `internal/rivercommon/river_common.go`).
- Run module tests from the module root that owns the changed files (`riverworkflow/`, repo root for `riverdriver`/`rivercommon`).

---

### Task 1: Metadata key for the wait spec

**Files:**
- Modify: `internal/rivercommon/river_common.go` (workflow metadata-key const block)
- Test: `internal/rivercommon/river_common_test.go` (create if absent)

**Interfaces:**
- Produces: `rivercommon.MetadataKeyWorkflowWait = "river:workflow_wait"` (string const). Consumed by Tasks 3 and 4.

- [ ] **Step 1: Write the failing test**

Create/append `internal/rivercommon/river_common_test.go`:

```go
package rivercommon

import "testing"

func TestMetadataKeyWorkflowWait(t *testing.T) {
	if MetadataKeyWorkflowWait != "river:workflow_wait" {
		t.Fatalf("unexpected key: %q", MetadataKeyWorkflowWait)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/rivercommon/ -run TestMetadataKeyWorkflowWait`
Expected: FAIL — `undefined: MetadataKeyWorkflowWait`.

- [ ] **Step 3: Add the constant**

In `internal/rivercommon/river_common.go`, inside the existing workflow metadata-key `const (...)` block, alongside `MetadataKeyWorkflowDeps`:

```go
	// MetadataKeyWorkflowWait holds the JSON-serialized WaitSpec for a task.
	// A task carrying this key is held pending by the dep-promotion SQL and
	// promoted only by the workflow scheduler once its wait resolves.
	MetadataKeyWorkflowWait = "river:workflow_wait"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/rivercommon/ -run TestMetadataKeyWorkflowWait`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/rivercommon/river_common.go internal/rivercommon/river_common_test.go
git commit -m "Add river:workflow_wait metadata key"
```

---

### Task 2: Wait/Timer types and structural Validate

**Files:**
- Create: `riverworkflow/wait.go`
- Test: `riverworkflow/wait_test.go`

**Interfaces:**
- Produces (consumed by Task 3 and Checkpoints 2–4):
  - `type WaitSpec struct { Terms []WaitTermSpec; Expr string }`
  - `func (s *WaitSpec) Validate() error`
  - `type WaitTermSpec struct { Name, Kind, Key, CELExpr, LabelText string; Timer *TimerSpec }`
  - `func (t WaitTermSpec) Label(string) WaitTermSpec`
  - `func WaitTermSignal(name, key, celExpr string) WaitTermSpec`
  - `func WaitTermTimer(spec TimerSpec) WaitTermSpec`
  - `func WaitTerm(name, celExpr string) WaitTermSpec`
  - `type TimerSpec struct { Name, Kind string; At time.Time; Dur time.Duration; DepTaskName string }`
  - `func TimerAt(name string, t time.Time) TimerSpec`
  - `func TimerAfterWaitStarted(name string, d time.Duration) TimerSpec`
  - `func TimerAfterWorkflowCreated(name string, d time.Duration) TimerSpec`
  - `func TimerAfterTaskFinalized(name, depTaskName string, d time.Duration) TimerSpec`
  - Kind consts: `WaitTermKindSignal/Timer/Generic`, `TimerKindAt/AfterWaitStarted/AfterWorkflowCreated/AfterTaskFinalized`
  - Errors: `ErrWaitExprEmpty`, `ErrWaitTermNameEmpty`, `ErrWaitTermNameDuplicate`, `ErrWaitTimerAnchorInvalid`

- [ ] **Step 1: Write the failing tests**

Create `riverworkflow/wait_test.go`:

```go
package riverworkflow

import (
	"errors"
	"testing"
	"time"
)

func TestWaitSpecValidate(t *testing.T) {
	t.Run("ValidSignalAndTimer", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{
				WaitTermSignal("approved", "approved", "payload.ok"),
				WaitTermTimer(TimerAfterWaitStarted("deadline", time.Hour)),
			},
			Expr: "approved || deadline",
		}
		if err := s.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("EmptyExpr", func(t *testing.T) {
		s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("t", "true")}, Expr: ""}
		if !errors.Is(s.Validate(), ErrWaitExprEmpty) {
			t.Fatalf("want ErrWaitExprEmpty, got %v", s.Validate())
		}
	})

	t.Run("EmptyTermName", func(t *testing.T) {
		s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("", "true")}, Expr: "x"}
		if !errors.Is(s.Validate(), ErrWaitTermNameEmpty) {
			t.Fatalf("want ErrWaitTermNameEmpty, got %v", s.Validate())
		}
	})

	t.Run("DuplicateTermName", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTerm("a", "true"), WaitTerm("a", "false")},
			Expr:  "a",
		}
		if !errors.Is(s.Validate(), ErrWaitTermNameDuplicate) {
			t.Fatalf("want ErrWaitTermNameDuplicate, got %v", s.Validate())
		}
	})

	t.Run("TimerAfterTaskFinalizedNeedsDep", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerAfterTaskFinalized("d", "", time.Minute))},
			Expr:  "d",
		}
		if !errors.Is(s.Validate(), ErrWaitTimerAnchorInvalid) {
			t.Fatalf("want ErrWaitTimerAnchorInvalid, got %v", s.Validate())
		}
	})
}

func TestWaitTermBuilders(t *testing.T) {
	sig := WaitTermSignal("n", "k", "payload.ok").Label("human approval")
	if sig.Kind != WaitTermKindSignal || sig.Key != "k" || sig.LabelText != "human approval" {
		t.Fatalf("signal term wrong: %+v", sig)
	}
	tim := WaitTermTimer(TimerAt("when", time.Unix(0, 0)))
	if tim.Kind != WaitTermKindTimer || tim.Timer == nil || tim.Timer.Kind != TimerKindAt {
		t.Fatalf("timer term wrong: %+v", tim)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd riverworkflow && go test ./... -run 'TestWaitSpecValidate|TestWaitTermBuilders'`
Expected: FAIL — undefined identifiers.

- [ ] **Step 3: Write the implementation**

Create `riverworkflow/wait.go`:

```go
package riverworkflow

import (
	"errors"
	"fmt"
	"time"
)

// Wait term kinds.
const (
	WaitTermKindSignal  = "signal"
	WaitTermKindTimer   = "timer"
	WaitTermKindGeneric = "generic"
)

// Timer anchor kinds.
const (
	TimerKindAt                  = "at"
	TimerKindAfterWaitStarted    = "after_wait_started"
	TimerKindAfterWorkflowCreated = "after_workflow_created"
	TimerKindAfterTaskFinalized  = "after_task_finalized"
)

var (
	ErrWaitExprEmpty          = errors.New("riverworkflow: wait spec Expr is empty")
	ErrWaitTermNameEmpty      = errors.New("riverworkflow: wait term name is empty")
	ErrWaitTermNameDuplicate  = errors.New("riverworkflow: duplicate wait term name")
	ErrWaitTimerAnchorInvalid = errors.New("riverworkflow: invalid timer anchor")
)

// TimerSpec describes a time anchor for a timer wait term. Construct via the
// Timer* builders rather than directly.
type TimerSpec struct {
	Name        string
	Kind        string
	At          time.Time     // TimerKindAt
	Dur         time.Duration // relative kinds
	DepTaskName string        // TimerKindAfterTaskFinalized
}

func TimerAt(name string, t time.Time) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAt, At: t}
}
func TimerAfterWaitStarted(name string, d time.Duration) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAfterWaitStarted, Dur: d}
}
func TimerAfterWorkflowCreated(name string, d time.Duration) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAfterWorkflowCreated, Dur: d}
}
func TimerAfterTaskFinalized(name, depTaskName string, d time.Duration) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAfterTaskFinalized, DepTaskName: depTaskName, Dur: d}
}

// WaitTermSpec is a single named predicate within a WaitSpec.
type WaitTermSpec struct {
	Name      string
	Kind      string
	Key       string     // signal key (signal terms)
	CELExpr   string     // signal-scoped or generic CEL
	Timer     *TimerSpec // timer terms
	LabelText string
}

// Label sets a human-readable label and returns the term for chaining.
func (t WaitTermSpec) Label(s string) WaitTermSpec { t.LabelText = s; return t }

func WaitTermSignal(name, key, celExpr string) WaitTermSpec {
	return WaitTermSpec{Name: name, Kind: WaitTermKindSignal, Key: key, CELExpr: celExpr}
}
func WaitTermTimer(spec TimerSpec) WaitTermSpec {
	return WaitTermSpec{Name: spec.Name, Kind: WaitTermKindTimer, Timer: &spec}
}
func WaitTerm(name, celExpr string) WaitTermSpec {
	return WaitTermSpec{Name: name, Kind: WaitTermKindGeneric, CELExpr: celExpr}
}

// WaitSpec is a CEL boolean expression over named terms; a wait-bearing task
// is promoted only when Expr evaluates true.
type WaitSpec struct {
	Terms []WaitTermSpec
	Expr  string
}

// Validate performs structural validation. NOTE: CEL syntax validation of
// Expr and term CELExpr is deferred to Checkpoint 2 (waiteval). // PARITY: CEL-syntax check pending.
func (s *WaitSpec) Validate() error {
	if s.Expr == "" {
		return ErrWaitExprEmpty
	}
	seen := make(map[string]struct{}, len(s.Terms))
	for _, t := range s.Terms {
		if t.Name == "" {
			return ErrWaitTermNameEmpty
		}
		if _, dup := seen[t.Name]; dup {
			return fmt.Errorf("%w: %q", ErrWaitTermNameDuplicate, t.Name)
		}
		seen[t.Name] = struct{}{}
		if t.Kind == WaitTermKindTimer {
			if t.Timer == nil {
				return fmt.Errorf("%w: term %q has nil timer", ErrWaitTimerAnchorInvalid, t.Name)
			}
			if t.Timer.Kind == TimerKindAfterTaskFinalized && t.Timer.DepTaskName == "" {
				return fmt.Errorf("%w: term %q requires a dep task name", ErrWaitTimerAnchorInvalid, t.Name)
			}
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd riverworkflow && go test ./... -run 'TestWaitSpecValidate|TestWaitTermBuilders'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/wait.go riverworkflow/wait_test.go
git commit -m "Add WaitSpec/WaitTermSpec/TimerSpec types and structural Validate"
```

---

### Task 3: Wire Wait into the workflow builder

**Files:**
- Modify: `riverworkflow/workflow.go` (`WorkflowTaskOpts`, `WorkflowTask`, `Add`, `validate`, `renderTaskOpts`)
- Test: `riverworkflow/workflow_test.go` (append)

**Interfaces:**
- Consumes: `rivercommon.MetadataKeyWorkflowWait` (Task 1); `WaitSpec` + `Validate` (Task 2).
- Produces: `WorkflowTaskOpts.Wait *WaitSpec`; wait-bearing tasks render with `Pending = true` and metadata key `river:workflow_wait` set to the JSON of the spec; `Prepare` returns `Validate`'s error for an invalid spec.

- [ ] **Step 1: Write the failing test**

Append to `riverworkflow/workflow_test.go`:

```go
func TestWorkflowWaitMetadata(t *testing.T) {
	w := newWorkflow[any](&WorkflowOpts{ID: "wf-wait"}, nil, "")
	w.Add("gate", &testJobArgs{}, nil, &WorkflowTaskOpts{
		Wait: &WaitSpec{
			Terms: []WaitTermSpec{WaitTermSignal("ok", "ok", "payload.ok")},
			Expr:  "ok",
		},
	})

	res, err := w.Prepare(context.Background())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	job := res.Jobs[0]
	if job.InsertOpts == nil || !job.InsertOpts.Pending {
		t.Fatalf("wait-bearing task must be Pending")
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(job.InsertOpts.Metadata, &meta); err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if _, ok := meta[rivercommon.MetadataKeyWorkflowWait]; !ok {
		t.Fatalf("expected %s in metadata, got %v", rivercommon.MetadataKeyWorkflowWait, meta)
	}
}

func TestWorkflowWaitInvalidRejected(t *testing.T) {
	w := newWorkflow[any](&WorkflowOpts{ID: "wf-bad"}, nil, "")
	w.Add("gate", &testJobArgs{}, nil, &WorkflowTaskOpts{
		Wait: &WaitSpec{Terms: []WaitTermSpec{WaitTerm("a", "true")}, Expr: ""},
	})
	if _, err := w.Prepare(context.Background()); !errors.Is(err, ErrWaitExprEmpty) {
		t.Fatalf("want ErrWaitExprEmpty, got %v", err)
	}
}
```

> If `testJobArgs`/`context`/`json`/`errors`/`rivercommon` imports are not already present in the test file, add them. Check existing helpers in `riverworkflow/workflow_test.go` and reuse the existing test JobArgs type name if it differs from `testJobArgs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd riverworkflow && go test ./... -run 'TestWorkflowWait'`
Expected: FAIL — `WorkflowTaskOpts` has no field `Wait`.

- [ ] **Step 3: Implement the wiring**

In `riverworkflow/workflow.go`:

1. Add to `WorkflowTaskOpts` (after `IgnoreDiscardedDeps`):

```go
	// Wait gates this task behind a CEL expression over signals, timers, and
	// dependency outputs. A wait-bearing task stays pending until the workflow
	// scheduler resolves the wait, independent of dependency completion.
	Wait *WaitSpec
```

2. Add to `WorkflowTask` struct:

```go
	wait *WaitSpec
```

3. In `Add`, capture it. Extend the `if taskOpts != nil {` block:

```go
	var wait *WaitSpec
	if taskOpts != nil {
		deps = append([]string(nil), taskOpts.Deps...)
		igC = taskOpts.IgnoreCancelledDeps
		igDc = taskOpts.IgnoreDiscardedDeps
		igDe = taskOpts.IgnoreDeletedDeps
		wait = taskOpts.Wait
	}
```

and set `wait: wait,` in the `&WorkflowTask{...}` literal.

4. In `validate()`, inside the existing `for _, t := range w.tasks` name/dup loop (after the duplicate check), validate the wait:

```go
		if t.wait != nil {
			if err := t.wait.Validate(); err != nil {
				return fmt.Errorf("task %q: %w", t.Name, err)
			}
		}
```

5. In `renderTaskOpts`, after the deps block (`if len(t.deps) > 0 { ... opts.Pending = true }`):

```go
	if t.wait != nil {
		inject(rivercommon.MetadataKeyWorkflowWait, t.wait)
		opts.Pending = true
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd riverworkflow && go test ./... -run 'TestWorkflowWait'`
Expected: PASS.

- [ ] **Step 5: Run the full package to check no regressions**

Run: `cd riverworkflow && go test ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add riverworkflow/workflow.go riverworkflow/workflow_test.go
git commit -m "Attach WaitSpec to workflow tasks and render into metadata"
```

---

### Task 4: Dep-promotion SQL skips wait-bearing tasks (all drivers)

**Files:**
- Modify: `riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql` (`JobUpdateWorkflowReady`, `candidates` CTE) — regenerates pgx **and** databasesql
- Modify: `riverdriver/riversqlite/internal/dbsqlc/river_job.sql` (`JobClassifyWorkflowReady`, `candidates` CTE)
- Regenerate: `riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql.go`, `riverdriver/riverdatabasesql/internal/dbsqlc/river_job.sql.go`, `riverdriver/riversqlite/internal/dbsqlc/river_job.sql.go` (via `make generate/sqlc`)
- Test: `riverdriver/riverdrivertest/job_workflow.go` (new exercise) + registration in `riverdriver/riverdrivertest/riverdrivertest.go`

**Interfaces:**
- Consumes: metadata key `river:workflow_wait` (Task 1).
- Produces: a conformance exercise `exerciseJobUpdateWorkflowReadySkipsWait` proving wait-bearing pending tasks with completed deps are NOT promoted, while a non-wait sibling IS.

- [ ] **Step 1: Write the failing conformance test**

In `riverdriver/riverdrivertest/job_workflow.go`, add (mirroring the existing `exerciseJobUpdateWorkflowReady` structure and the `insertWorkflowJob` helper, which already accepts metadata via `workflowJobOpts`):

```go
func exerciseJobUpdateWorkflowReadySkipsWait[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()
	t.Run("JobUpdateWorkflowReady_SkipsWaitBearingTasks", func(t *testing.T) {
		exec, _ := executorWithTx(ctx, t)
		workflowID := "wf-" + t.Name()

		// Completed dependency.
		insertWorkflowJob(ctx, t, exec, workflowJobOpts{
			WorkflowID: workflowID, TaskName: "dep", State: rivertype.JobStateCompleted,
		})
		// Non-wait dependent: should promote.
		plain := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
			WorkflowID: workflowID, TaskName: "plain", Deps: []string{"dep"}, State: rivertype.JobStatePending,
		})
		// Wait-bearing dependent: must stay pending.
		waiter := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
			WorkflowID: workflowID, TaskName: "waiter", Deps: []string{"dep"}, State: rivertype.JobStatePending,
			Wait: json.RawMessage(`{"Terms":[],"Expr":"true"}`),
		})

		_, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: time.Now()})
		require.NoError(t, err)

		plainAfter, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: plain.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, plainAfter.State, "non-wait task should promote")

		waiterAfter, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: waiter.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStatePending, waiterAfter.State, "wait-bearing task must stay pending")
	})
}
```

> Extend the `workflowJobOpts` struct and `insertWorkflowJob` helper with a `Wait json.RawMessage` field that, when non-nil, sets `metadata[rivercommon.MetadataKeyWorkflowWait] = <raw>`. Follow the existing pattern used for `Deps`. Confirm the exact `JobGetByID` param/method name in `riverdriver/river_driver_interface.go` and adjust if it differs.

Register it in `riverdriver/riverdrivertest/riverdrivertest.go` `Exercise()`, next to `exerciseJobUpdateWorkflowReady(...)`:

```go
	exerciseJobUpdateWorkflowReadySkipsWait(ctx, t, executorWithTx)
```

- [ ] **Step 2: Run against pgx to verify it fails**

Run: `go test ./riverdriver/riverpgxv5/... -run 'Exercise.*SkipsWait' -v` (or the package's standard conformance entrypoint).
Expected: FAIL — wait-bearing task is promoted to `available` (skip predicate not yet added).

- [ ] **Step 3: Add the Postgres skip predicate**

In `riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql`, in `JobUpdateWorkflowReady`'s `candidates` CTE, change:

```sql
  WHERE state = 'pending'
    AND metadata ? 'river:workflow_id'
```
to:
```sql
  WHERE state = 'pending'
    AND metadata ? 'river:workflow_id'
    AND NOT (metadata ? 'river:workflow_wait')
```

- [ ] **Step 4: Add the SQLite skip predicate**

In `riverdriver/riversqlite/internal/dbsqlc/river_job.sql`, in `JobClassifyWorkflowReady`'s `candidates` CTE, change:

```sql
  WHERE j.state = 'pending'
    AND json_extract(j.metadata, '$."river:workflow_id"') IS NOT NULL
```
to:
```sql
  WHERE j.state = 'pending'
    AND json_extract(j.metadata, '$."river:workflow_id"') IS NOT NULL
    AND json_extract(j.metadata, '$."river:workflow_wait"') IS NULL
```

- [ ] **Step 5: Regenerate sqlc**

Run: `make generate/sqlc`
Expected: `river_job.sql.go` updated under all three drivers (pgx, databasesql, sqlite). Then:
Run: `make verify/sqlc`
Expected: no diff.

- [ ] **Step 6: Run conformance across all three drivers**

Run:
```bash
go test ./riverdriver/riverpgxv5/... -run Exercise
go test ./riverdriver/riverdatabasesql/... -run Exercise
go test ./riverdriver/riversqlite/... -run Exercise
```
Expected: PASS, including `JobUpdateWorkflowReady_SkipsWaitBearingTasks`.

- [ ] **Step 7: Commit**

```bash
git add riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql \
        riverdriver/riversqlite/internal/dbsqlc/river_job.sql \
        riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql.go \
        riverdriver/riverdatabasesql/internal/dbsqlc/river_job.sql.go \
        riverdriver/riversqlite/internal/dbsqlc/river_job.sql.go \
        riverdriver/riverdrivertest/job_workflow.go \
        riverdriver/riverdrivertest/riverdrivertest.go
git commit -m "Skip wait-bearing tasks in dep-promotion SQL across all drivers"
```

---

## Checkpoint exit criteria

- A workflow task with `WorkflowTaskOpts.Wait` set inserts as `pending` with `river:workflow_wait` metadata.
- `JobUpdateWorkflowReady` / `JobClassifyWorkflowReady` never promote a wait-bearing task, even with all deps completed — proven by conformance on all three drivers.
- Non-wait tasks promote exactly as before (existing conformance still green).
- `WaitSpec.Validate()` rejects empty Expr, empty/duplicate term names, and a finalized-timer term missing its dep.
- `make verify/sqlc` clean; `go test ./...` green in `riverworkflow/` and the three driver packages.

## Hand-off to Checkpoint 2

Wait-bearing tasks are now *held* but never *released* (no evaluator yet). Checkpoint 2 adds the `waiteval` CEL engine, timer anchor resolution, and the scheduler pass that evaluates timer-only waits and promotes via a new `JobPromoteWorkflowTask`. Its plan: `docs/superpowers/plans/2026-06-23-workflow-wait-checkpoint2-timers-cel.md` (to be written).

## Self-review notes

- **Spec coverage (Checkpoint 1 slice of §11.1):** metadata key ✓ (Task 1), WaitSpec types + Validate ✓ (Task 2), `WorkflowTaskOpts.Wait` + metadata injection + Pending ✓ (Task 3), SQL skip across 3 drivers + conformance ✓ (Task 4). Scheduler no-op pass deferred to Checkpoint 2 (it needs the fetch/promote driver methods, which belong with the evaluator) — this does not affect Checkpoint 1's exit criteria since held tasks simply remain pending.
- **Deferred (marked):** CEL-syntax validation in `Validate` (`// PARITY: CEL-syntax check pending`) lands in Checkpoint 2 with `waiteval`.
- **Type consistency:** `MetadataKeyWorkflowWait`, `WaitSpec{Terms,Expr}`, `WaitTermSpec`, `TimerSpec`, and the builder names are identical across Tasks 1–4 and the spec's §3.
- **Verify-before-real-code caveats:** the test helpers (`testJobArgs` name, `JobGetByID` param/method) must be confirmed against the actual files during execution — flagged inline at their use sites.
