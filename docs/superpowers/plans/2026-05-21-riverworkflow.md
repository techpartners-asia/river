# Riverworkflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the `riverworkflow` submodule providing fan-out / fan-in workflow DAGs on top of OSS River, with API mirroring riverpro's `Workflow` surface.

**Architecture:** New Go module `riverworkflow/` wraps `*river.Client` and adds workflow-building, workflow-cancellation, and dependency-loading methods. Tasks are inserted into River's `river_job` table in state `pending` with workflow metadata. A new leader-elected `WorkflowScheduler` maintenance service polls pending workflow tasks and promotes them to `available` once their deps complete. Wiring into River's leader-elected maintainer is done via the existing `pilotPlugin` interface: riverworkflow wraps the user's driver so its custom pilot returns the scheduler as a `PluginMaintenanceServices()` entry. New driver methods are added to `riverdriver` and implemented in all three concrete drivers (riverpgxv5, riverdatabasesql, riversqlite). A migration adds an index on `metadata->>'river:workflow_id'` for each driver's `main` line.

**Tech Stack:** Go 1.25, `pgx/v5`, `database/sql`, `mattn/go-sqlite3`, `sqlc`, River's `startstop`/`baseservice`/`riverpilot` packages.

**Reference spec:** `docs/superpowers/specs/2026-05-21-riverworkflow-design.md`

**Conventions reminder** (from `AGENTS.md`):
- Use `make test`, `make lint`, `make generate` for testing/linting/sqlc generation.
- All tests start with `t.Parallel()`.
- Use the local `testBundle` + `setup(t)` pattern.
- Use `require`, not `assert`.
- Imports: gci sections Standard / Default / `github.com/riverqueue`.
- Alphabetical ordering for new types/fields/methods.

---

## Phase 0 — Module bootstrap

### Task 1: Create the `riverworkflow` module

**Files:**
- Create: `riverworkflow/go.mod`
- Create: `riverworkflow/doc.go`
- Modify: `go.work`

- [ ] **Step 1: Create `riverworkflow/go.mod`**

```
module github.com/riverqueue/river/riverworkflow

go 1.25.0

toolchain go1.25.7

require (
	github.com/riverqueue/river v0.37.1
	github.com/riverqueue/river/riverdriver v0.37.1
	github.com/riverqueue/river/rivershared v0.37.1
	github.com/riverqueue/river/rivertype v0.37.1
	github.com/stretchr/testify v1.11.1
)
```

- [ ] **Step 2: Create `riverworkflow/doc.go`**

```go
// Package riverworkflow provides fan-out / fan-in workflow DAGs on top of
// [github.com/riverqueue/river]. A workflow is a set of named tasks with
// declared dependencies; tasks become eligible to run only after their
// dependencies complete successfully.
//
// See the README for a usage walkthrough and the godoc on
// [Client.NewWorkflow] for the API entry point.
package riverworkflow
```

- [ ] **Step 3: Add the module to `go.work`**

```
go 1.25.0

toolchain go1.25.7

use (
	.
	./cmd/river
	./riverdriver
	./riverdriver/riverdatabasesql
	./riverdriver/riverdrivertest
	./riverdriver/riverpgxv5
	./riverdriver/riversqlite
	./rivershared
	./rivertype
	./riverworkflow
)
```

- [ ] **Step 4: Verify the module builds**

Run: `cd riverworkflow && go build ./...`
Expected: no output, exit 0.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/go.mod riverworkflow/doc.go go.work
git commit -m "Bootstrap riverworkflow module"
```

---

### Task 2: Add workflow metadata keys to `rivercommon`

**Files:**
- Modify: `internal/rivercommon/river_common.go`

- [ ] **Step 1: Write the failing test**

Create `internal/rivercommon/river_common_test.go` if absent, otherwise append:

```go
func TestMetadataKeyWorkflow(t *testing.T) {
	t.Parallel()

	require.Equal(t, "river:workflow_id", MetadataKeyWorkflowID)
	require.Equal(t, "river:workflow_name", MetadataKeyWorkflowName)
	require.Equal(t, "river:workflow_task", MetadataKeyWorkflowTask)
	require.Equal(t, "river:workflow_deps", MetadataKeyWorkflowDeps)
	require.Equal(t, "river:workflow_ignore_cancelled_deps", MetadataKeyWorkflowIgnoreCancelledDeps)
	require.Equal(t, "river:workflow_ignore_discarded_deps", MetadataKeyWorkflowIgnoreDiscardedDeps)
	require.Equal(t, "river:workflow_ignore_deleted_deps", MetadataKeyWorkflowIgnoreDeletedDeps)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/rivercommon -run TestMetadataKeyWorkflow -v`
Expected: FAIL with `undefined: MetadataKeyWorkflowID`.

- [ ] **Step 3: Add the constants to `internal/rivercommon/river_common.go`**

Append to the existing `const (...)` block (keep alphabetical order in the file):

```go
const (
	// existing constants ...

	// MetadataKeyWorkflowDeps holds a JSON array of task names that a
	// workflow task depends on.
	MetadataKeyWorkflowDeps = "river:workflow_deps"

	// MetadataKeyWorkflowID identifies the workflow a task belongs to.
	MetadataKeyWorkflowID = "river:workflow_id"

	// MetadataKeyWorkflowIgnoreCancelledDeps, when set to true, causes a
	// cancelled dep to be treated as a successful dep for promotion.
	MetadataKeyWorkflowIgnoreCancelledDeps = "river:workflow_ignore_cancelled_deps"

	// MetadataKeyWorkflowIgnoreDeletedDeps mirrors the above for deleted deps.
	MetadataKeyWorkflowIgnoreDeletedDeps = "river:workflow_ignore_deleted_deps"

	// MetadataKeyWorkflowIgnoreDiscardedDeps mirrors the above for discarded deps.
	MetadataKeyWorkflowIgnoreDiscardedDeps = "river:workflow_ignore_discarded_deps"

	// MetadataKeyWorkflowName is an optional human-readable workflow label.
	MetadataKeyWorkflowName = "river:workflow_name"

	// MetadataKeyWorkflowTask is the unique-within-workflow task name.
	MetadataKeyWorkflowTask = "river:workflow_task"
)
```

- [ ] **Step 4: Run tests again**

Run: `go test ./internal/rivercommon -run TestMetadataKeyWorkflow -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/rivercommon/river_common.go internal/rivercommon/river_common_test.go
git commit -m "Add workflow metadata key constants to rivercommon"
```

---

## Phase 1 — Driver interface

### Task 3: Add workflow params types and methods to `riverdriver.Executor`

**Files:**
- Modify: `riverdriver/river_driver_interface.go`

- [ ] **Step 1: Add params types**

Add these types alongside the existing `Job*Params` structs (keep alphabetical order):

```go
type JobCancelWorkflowParams struct {
	Schema     string
	WorkflowID string
	Now        time.Time
	Reason     string
}

type JobGetWorkflowTasksParams struct {
	Schema     string
	WorkflowID string
	TaskNames  []string // optional filter; empty means "all tasks"
}

type JobUpdateWorkflowReadyParams struct {
	Schema string
	Max    int
	Now    time.Time
}
```

- [ ] **Step 2: Add the three methods to the `Executor` interface**

Inside the `type Executor interface { ... }` block in the same file, in alphabetical order:

```go
JobCancelWorkflow(ctx context.Context, params *JobCancelWorkflowParams) ([]*rivertype.JobRow, error)
JobGetWorkflowTasks(ctx context.Context, params *JobGetWorkflowTasksParams) ([]*rivertype.JobRow, error)
JobUpdateWorkflowReady(ctx context.Context, params *JobUpdateWorkflowReadyParams) ([]*rivertype.JobRow, error)
```

- [ ] **Step 3: Run lint to confirm signature shape**

Run: `cd riverdriver && go build ./...`
Expected: compile error in the three concrete drivers (they don't implement the new methods yet). That's correct — it confirms the interface change took effect. They get implemented in Tasks 8-13.

- [ ] **Step 4: Commit**

```bash
git add riverdriver/river_driver_interface.go
git commit -m "Add workflow Executor methods to riverdriver interface"
```

---

### Task 4: Add unimplemented stubs to the three concrete drivers

This task keeps `go build` passing while the real implementations land in Tasks 8-13. The stubs return `riverdriver.ErrNotImplemented`. Each test for the real method (Task 7's conformance suite) will fail against these stubs until the corresponding driver task lands.

**Files:**
- Modify: `riverdriver/riverpgxv5/river_pgx_v5_driver.go`
- Modify: `riverdriver/riverdatabasesql/river_database_sql_driver.go`
- Modify: `riverdriver/riversqlite/river_sqlite_driver.go`

- [ ] **Step 1: Append stub methods to each driver's executor type**

For all three drivers, locate the executor type that satisfies `riverdriver.Executor` (e.g. `Executor` in pgxv5). Add stubs:

```go
func (e *Executor) JobCancelWorkflow(ctx context.Context, params *riverdriver.JobCancelWorkflowParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobGetWorkflowTasks(ctx context.Context, params *riverdriver.JobGetWorkflowTasksParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobUpdateWorkflowReady(ctx context.Context, params *riverdriver.JobUpdateWorkflowReadyParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
```

The concrete name of the executor type may differ; grep for `JobCancel(ctx context.Context` in each driver file to find the right receiver.

- [ ] **Step 2: Build all modules**

Run: `make tidy && go build ./... && cd riverdriver && go build ./... && cd riverdatabasesql && go build ./... && cd ../riverpgxv5 && go build ./... && cd ../riversqlite && go build ./...`
Expected: all succeed.

- [ ] **Step 3: Commit**

```bash
git add riverdriver/riverpgxv5/river_pgx_v5_driver.go riverdriver/riverdatabasesql/river_database_sql_driver.go riverdriver/riversqlite/river_sqlite_driver.go
git commit -m "Add ErrNotImplemented stubs for new workflow driver methods"
```

---

### Task 5: Scaffold conformance tests in `riverdrivertest`

**Files:**
- Create: `riverdriver/riverdrivertest/workflow_test.go`

- [ ] **Step 1: Write the conformance test scaffold**

The scaffold defines table-driven tests that run against the shared `riverdrivertest.Suite` driver fixture. Test bodies are filled in via these subtests. (See `riverdrivertest/riverdrivertest.go` for `Suite[TTx]` and the existing `Exercise*` helpers it provides; mirror an existing exercise like `JobGetAvailable_*` for setup.)

```go
package riverdrivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

func ExerciseJobUpdateWorkflowReady[TTx any](
	ctx context.Context,
	t *testing.T,
	suite Suite[TTx],
) {
	t.Helper()

	t.Run("PromotesWhenAllDepsCompleted", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-promotes"
		taskA := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "a",
			State:      rivertype.JobStateCompleted,
		})
		_ = taskA
		taskB := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "b",
			Deps:       []string{"a"},
			State:      rivertype.JobStatePending,
		})

		updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
			Schema: bundle.Schema,
			Max:    100,
			Now:    bundle.Now,
		})
		require.NoError(t, err)
		require.Len(t, updated, 1)
		require.Equal(t, taskB.ID, updated[0].ID)
		require.Equal(t, rivertype.JobStateAvailable, updated[0].State)
	})

	t.Run("LeavesPendingWhenDepStillRunning", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-running-dep"
		_ = suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "a",
			State:      rivertype.JobStateRunning,
		})
		taskB := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "b",
			Deps:       []string{"a"},
			State:      rivertype.JobStatePending,
		})

		updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
			Schema: bundle.Schema,
			Max:    100,
			Now:    bundle.Now,
		})
		require.NoError(t, err)
		require.Empty(t, updated)

		row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{Schema: bundle.Schema, ID: taskB.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStatePending, row.State)
	})

	t.Run("CancelsWhenDepDiscarded", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-discarded"
		_ = suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "a",
			State:      rivertype.JobStateDiscarded,
		})
		taskB := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "b",
			Deps:       []string{"a"},
			State:      rivertype.JobStatePending,
		})

		updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
			Schema: bundle.Schema,
			Max:    100,
			Now:    bundle.Now,
		})
		require.NoError(t, err)
		require.Len(t, updated, 1)
		require.Equal(t, taskB.ID, updated[0].ID)
		require.Equal(t, rivertype.JobStateCancelled, updated[0].State)
		require.NotNil(t, updated[0].FinalizedAt)
	})

	t.Run("HonorsIgnoreDiscardedDeps", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-ignore-discarded"
		_ = suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "a",
			State:      rivertype.JobStateDiscarded,
		})
		taskB := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID:            workflowID,
			TaskName:              "b",
			Deps:                  []string{"a"},
			State:                 rivertype.JobStatePending,
			IgnoreDiscardedDeps:   true,
		})

		updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
			Schema: bundle.Schema,
			Max:    100,
			Now:    bundle.Now,
		})
		require.NoError(t, err)
		require.Len(t, updated, 1)
		require.Equal(t, taskB.ID, updated[0].ID)
		require.Equal(t, rivertype.JobStateAvailable, updated[0].State)
	})

	t.Run("ScheduledWhenScheduledAtInFuture", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-scheduled"
		_ = suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "a",
			State:      rivertype.JobStateCompleted,
		})
		taskB := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID:  workflowID,
			TaskName:    "b",
			Deps:        []string{"a"},
			State:       rivertype.JobStatePending,
			ScheduledAt: bundle.Now.Add(time.Hour),
		})

		updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
			Schema: bundle.Schema,
			Max:    100,
			Now:    bundle.Now,
		})
		require.NoError(t, err)
		require.Len(t, updated, 1)
		require.Equal(t, taskB.ID, updated[0].ID)
		require.Equal(t, rivertype.JobStateScheduled, updated[0].State)
	})

	t.Run("CancelsWhenDepMissingAndIgnoreDeletedFalse", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-missing-dep"
		taskB := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{
			WorkflowID: workflowID,
			TaskName:   "b",
			Deps:       []string{"a"}, // "a" never inserted
			State:      rivertype.JobStatePending,
		})

		updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
			Schema: bundle.Schema,
			Max:    100,
			Now:    bundle.Now,
		})
		require.NoError(t, err)
		require.Len(t, updated, 1)
		require.Equal(t, taskB.ID, updated[0].ID)
		require.Equal(t, rivertype.JobStateCancelled, updated[0].State)
	})
}

func ExerciseJobGetWorkflowTasks[TTx any](
	ctx context.Context,
	t *testing.T,
	suite Suite[TTx],
) {
	t.Helper()

	t.Run("ReturnsAllTasksForWorkflow", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-get-all"
		a := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
		b := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", State: rivertype.JobStateAvailable, Deps: []string{"a"}})

		// Insert an unrelated workflow row to confirm it's not returned.
		_ = suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{WorkflowID: "other-wf", TaskName: "a", State: rivertype.JobStateCompleted})

		rows, err := exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
			Schema:     bundle.Schema,
			WorkflowID: workflowID,
		})
		require.NoError(t, err)
		require.Len(t, rows, 2)
		ids := []int64{rows[0].ID, rows[1].ID}
		require.ElementsMatch(t, []int64{a.ID, b.ID}, ids)
	})

	t.Run("FiltersByTaskNames", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-filter"
		a := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
		_ = suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", State: rivertype.JobStateAvailable})

		rows, err := exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
			Schema:     bundle.Schema,
			WorkflowID: workflowID,
			TaskNames:  []string{"a"},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, a.ID, rows[0].ID)
	})
}

func ExerciseJobCancelWorkflow[TTx any](
	ctx context.Context,
	t *testing.T,
	suite Suite[TTx],
) {
	t.Helper()

	t.Run("CancelsNonFinalizedTasks", func(t *testing.T) {
		t.Parallel()
		exec, bundle := suite.SetupExecutor(t)

		workflowID := "wf-cancel"
		completed := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
		pending := suite.InsertWorkflowJob(t, exec, bundle, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", State: rivertype.JobStatePending, Deps: []string{"a"}})

		cancelled, err := exec.JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
			Schema:     bundle.Schema,
			WorkflowID: workflowID,
			Now:        bundle.Now,
			Reason:     "user requested",
		})
		require.NoError(t, err)
		require.Len(t, cancelled, 1)
		require.Equal(t, pending.ID, cancelled[0].ID)
		require.Equal(t, rivertype.JobStateCancelled, cancelled[0].State)

		// Completed row stays completed.
		row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{Schema: bundle.Schema, ID: completed.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, row.State)
	})
}

// workflowJobOpts and Suite.InsertWorkflowJob are added in this task too — see Step 2.
```

- [ ] **Step 2: Add `workflowJobOpts` and `Suite.InsertWorkflowJob` helper**

Append to `riverdrivertest/riverdrivertest.go` (or wherever `Suite[TTx]` lives — grep for `type Suite[`). Helper builds metadata JSON and inserts via `JobInsertFull`:

```go
type workflowJobOpts struct {
	Deps                []string
	IgnoreCancelledDeps bool
	IgnoreDeletedDeps   bool
	IgnoreDiscardedDeps bool
	ScheduledAt         time.Time
	State               rivertype.JobState
	TaskName            string
	WorkflowID          string
}

func (s Suite[TTx]) InsertWorkflowJob(t *testing.T, exec riverdriver.Executor, bundle *SuiteBundle, opts workflowJobOpts) *rivertype.JobRow {
	t.Helper()

	metadata := map[string]any{
		rivercommon.MetadataKeyWorkflowID:   opts.WorkflowID,
		rivercommon.MetadataKeyWorkflowTask: opts.TaskName,
	}
	if len(opts.Deps) > 0 {
		metadata[rivercommon.MetadataKeyWorkflowDeps] = opts.Deps
	}
	if opts.IgnoreCancelledDeps {
		metadata[rivercommon.MetadataKeyWorkflowIgnoreCancelledDeps] = true
	}
	if opts.IgnoreDiscardedDeps {
		metadata[rivercommon.MetadataKeyWorkflowIgnoreDiscardedDeps] = true
	}
	if opts.IgnoreDeletedDeps {
		metadata[rivercommon.MetadataKeyWorkflowIgnoreDeletedDeps] = true
	}
	metadataBytes, err := json.Marshal(metadata)
	require.NoError(t, err)

	scheduledAt := opts.ScheduledAt
	if scheduledAt.IsZero() {
		scheduledAt = bundle.Now
	}

	var finalizedAt *time.Time
	if opts.State == rivertype.JobStateCancelled ||
		opts.State == rivertype.JobStateCompleted ||
		opts.State == rivertype.JobStateDiscarded {
		ft := bundle.Now
		finalizedAt = &ft
	}

	row, err := exec.JobInsertFull(s.Ctx, &riverdriver.JobInsertFullParams{
		Schema:      bundle.Schema,
		Kind:        "test_workflow_kind",
		EncodedArgs: []byte(`{}`),
		FinalizedAt: finalizedAt,
		Metadata:    metadataBytes,
		Priority:    1,
		Queue:       rivercommon.QueueDefault,
		ScheduledAt: scheduledAt,
		State:       opts.State,
		Tags:        []string{},
	})
	require.NoError(t, err)
	return row
}
```

If `Suite[TTx]` already has a `Ctx` and `SuiteBundle.Schema`, use those; otherwise add the field. Adjust the call to `JobInsertFull` to match the actual params struct used in the codebase (grep `JobInsertFullParams` to confirm fields).

- [ ] **Step 3: Wire the new exercises into the existing test entry-points**

Open `riverdriver/riverdrivertest/riverdrivertest.go` and find the function that runs all `Exercise*` tests against a driver (likely named `ExerciseExecutorFull` or similar — grep `func Exercise` for the list). Add three new top-level calls:

```go
t.Run("JobCancelWorkflow", func(t *testing.T) { ExerciseJobCancelWorkflow(ctx, t, suite) })
t.Run("JobGetWorkflowTasks", func(t *testing.T) { ExerciseJobGetWorkflowTasks(ctx, t, suite) })
t.Run("JobUpdateWorkflowReady", func(t *testing.T) { ExerciseJobUpdateWorkflowReady(ctx, t, suite) })
```

- [ ] **Step 4: Run the conformance tests against the (stubbed) drivers**

Run: `cd riverdriver/riverpgxv5 && go test ./... -run TestDriver -v`
Expected: the three new exercises FAIL (returning `ErrNotImplemented`). That confirms the test plumbing works.

- [ ] **Step 5: Commit**

```bash
git add riverdriver/riverdrivertest/
git commit -m "Add workflow conformance tests to riverdrivertest"
```

---

## Phase 2 — Driver implementations

### Task 6: Implement `riverpgxv5` workflow sqlc queries

**Files:**
- Modify: `riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql`
- Generate: `riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql.go` (via `make generate`)

- [ ] **Step 1: Append three queries to `river_job.sql`**

```sql
-- name: JobGetWorkflowTasks :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'river:workflow_id' = @workflow_id::text
  AND (
    cardinality(@task_names::text[]) = 0
    OR metadata->>'river:workflow_task' = ANY(@task_names::text[])
  )
ORDER BY id;

-- name: JobCancelWorkflow :many
WITH targets AS (
  SELECT id
  FROM /* TEMPLATE: schema */river_job
  WHERE metadata->>'river:workflow_id' = @workflow_id::text
    AND finalized_at IS NULL
  FOR UPDATE
),
updated AS (
  UPDATE /* TEMPLATE: schema */river_job
  SET state        = 'cancelled',
      finalized_at = @now::timestamptz,
      errors       = errors || jsonb_build_array(jsonb_build_object(
        'at',      @now::timestamptz,
        'attempt', attempt,
        'error',   @reason::text,
        'trace',   ''
      ))
  WHERE id IN (SELECT id FROM targets)
  RETURNING *
)
SELECT * FROM updated;

-- name: JobUpdateWorkflowReady :many
WITH candidates AS (
  SELECT id, metadata, scheduled_at
  FROM /* TEMPLATE: schema */river_job
  WHERE state = 'pending'
    AND metadata ? 'river:workflow_id'
  ORDER BY id
  FOR UPDATE SKIP LOCKED
  LIMIT @max::int
),
dep_states AS (
  SELECT
    c.id AS candidate_id,
    sib.state AS dep_state,
    sib.metadata->>'river:workflow_task' AS dep_task
  FROM candidates c
  LEFT JOIN /* TEMPLATE: schema */river_job sib
    ON sib.metadata->>'river:workflow_id' = c.metadata->>'river:workflow_id'
   AND sib.metadata->>'river:workflow_task' IN (
         SELECT jsonb_array_elements_text(c.metadata->'river:workflow_deps')
       )
),
resolved AS (
  SELECT
    c.id,
    c.scheduled_at,
    c.metadata,
    -- treat ignored states as effectively completed:
    bool_and(
      d.dep_state = 'completed'
      OR (d.dep_state = 'cancelled' AND COALESCE((c.metadata->>'river:workflow_ignore_cancelled_deps')::bool, false))
      OR (d.dep_state = 'discarded' AND COALESCE((c.metadata->>'river:workflow_ignore_discarded_deps')::bool, false))
    ) FILTER (WHERE d.dep_state IS NOT NULL) AS all_done,
    bool_or(
      d.dep_state = 'cancelled' AND NOT COALESCE((c.metadata->>'river:workflow_ignore_cancelled_deps')::bool, false)
    ) FILTER (WHERE d.dep_state IS NOT NULL) AS fail_cancelled,
    bool_or(
      d.dep_state = 'discarded' AND NOT COALESCE((c.metadata->>'river:workflow_ignore_discarded_deps')::bool, false)
    ) FILTER (WHERE d.dep_state IS NOT NULL) AS fail_discarded,
    count(d.dep_state) AS dep_rows_found,
    (SELECT count(*) FROM jsonb_array_elements_text(c.metadata->'river:workflow_deps')) AS dep_rows_declared
  FROM candidates c
  LEFT JOIN dep_states d ON d.candidate_id = c.id
  GROUP BY c.id, c.scheduled_at, c.metadata
),
classified AS (
  SELECT
    id,
    CASE
      WHEN fail_cancelled OR fail_discarded THEN 'cancelled'
      WHEN dep_rows_found < dep_rows_declared
        AND NOT COALESCE((metadata->>'river:workflow_ignore_deleted_deps')::bool, false) THEN 'cancelled'
      WHEN COALESCE(all_done, true) AND dep_rows_found >= dep_rows_declared
        AND scheduled_at > @now::timestamptz THEN 'scheduled'
      WHEN COALESCE(all_done, true) AND dep_rows_found >= dep_rows_declared THEN 'available'
      ELSE 'pending'
    END AS new_state
  FROM resolved
)
UPDATE /* TEMPLATE: schema */river_job j
SET state        = c.new_state::river_job_state,
    finalized_at = CASE WHEN c.new_state = 'cancelled' THEN @now::timestamptz ELSE j.finalized_at END
FROM classified c
WHERE j.id = c.id
  AND c.new_state <> 'pending'
RETURNING j.*;
```

- [ ] **Step 2: Regenerate sqlc**

Run: `make generate`
Expected: `river_job.sql.go` updated with new generated functions `JobGetWorkflowTasks`, `JobCancelWorkflow`, `JobUpdateWorkflowReady`.

- [ ] **Step 3: Commit the SQL + generated code**

```bash
git add riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql.go
git commit -m "Add workflow sqlc queries for riverpgxv5"
```

---

### Task 7: Wire `riverpgxv5` driver methods to the new sqlc queries

**Files:**
- Modify: `riverdriver/riverpgxv5/river_pgx_v5_driver.go` (replace the stubs from Task 4)

- [ ] **Step 1: Replace the stub methods**

Open `riverdriver/riverpgxv5/river_pgx_v5_driver.go`. Find the three stub methods added in Task 4 and replace each with a real implementation. (Look at adjacent methods like `JobCancel` for the receiver-name + dbsqlc-call pattern; the generated function names from Task 6 are `dbsqlc.New().JobGetWorkflowTasks`, etc.)

```go
func (e *Executor) JobCancelWorkflow(ctx context.Context, params *riverdriver.JobCancelWorkflowParams) ([]*rivertype.JobRow, error) {
	rows, err := dbsqlc.New().JobCancelWorkflow(ctx, e.dbtx, &dbsqlc.JobCancelWorkflowParams{
		Schema:     params.Schema,
		WorkflowID: params.WorkflowID,
		Now:        params.Now,
		Reason:     params.Reason,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceErr(rows, jobRowFromInternal)
}

func (e *Executor) JobGetWorkflowTasks(ctx context.Context, params *riverdriver.JobGetWorkflowTasksParams) ([]*rivertype.JobRow, error) {
	rows, err := dbsqlc.New().JobGetWorkflowTasks(ctx, e.dbtx, &dbsqlc.JobGetWorkflowTasksParams{
		Schema:     params.Schema,
		WorkflowID: params.WorkflowID,
		TaskNames:  params.TaskNames,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceErr(rows, jobRowFromInternal)
}

func (e *Executor) JobUpdateWorkflowReady(ctx context.Context, params *riverdriver.JobUpdateWorkflowReadyParams) ([]*rivertype.JobRow, error) {
	rows, err := dbsqlc.New().JobUpdateWorkflowReady(ctx, e.dbtx, &dbsqlc.JobUpdateWorkflowReadyParams{
		Schema: params.Schema,
		Max:    int32(params.Max),
		Now:    params.Now,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceErr(rows, jobRowFromInternal)
}
```

(The exact helper names — `interpretError`, `mapSliceErr`, `jobRowFromInternal` — are whatever exists in the file. Grep `jobRowFromInternal` or similar to find the right converter.)

- [ ] **Step 2: Run conformance tests**

Run: `cd riverdriver/riverpgxv5 && go test ./... -run "TestDriver/JobUpdateWorkflowReady|TestDriver/JobGetWorkflowTasks|TestDriver/JobCancelWorkflow" -v`
Expected: PASS (all six subtests across three exercises).

- [ ] **Step 3: Commit**

```bash
git add riverdriver/riverpgxv5/river_pgx_v5_driver.go
git commit -m "Implement workflow Executor methods for riverpgxv5"
```

---

### Task 8: Implement `riverdatabasesql` workflow sqlc + driver methods

**Files:**
- Modify: `riverdriver/riverdatabasesql/internal/dbsqlc/river_job.sql`
- Generate: corresponding `.sql.go`
- Modify: `riverdriver/riverdatabasesql/river_database_sql_driver.go`

- [ ] **Step 1: Add the same three queries to `riverdriver/riverdatabasesql/internal/dbsqlc/river_job.sql`**

Use the same SQL as Task 6 — Postgres syntax is identical for both drivers.

- [ ] **Step 2: Regenerate**

Run: `make generate`
Expected: `riverdriver/riverdatabasesql/internal/dbsqlc/river_job.sql.go` updated.

- [ ] **Step 3: Replace the three stub methods in `river_database_sql_driver.go`**

Mirror Task 7's structure, calling the database/sql-generated query functions. (Grep for an existing method like `JobCancel` to confirm the helper names — they typically differ slightly between drivers.)

- [ ] **Step 4: Run conformance tests**

Run: `cd riverdriver/riverdatabasesql && go test ./... -run "TestDriver/JobUpdateWorkflowReady|TestDriver/JobGetWorkflowTasks|TestDriver/JobCancelWorkflow" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverdriver/riverdatabasesql/
git commit -m "Implement workflow Executor methods for riverdatabasesql"
```

---

### Task 9: Implement `riversqlite` workflow sqlc + driver methods

**Files:**
- Modify: `riverdriver/riversqlite/internal/dbsqlc/river_job.sql`
- Generate: corresponding `.sql.go`
- Modify: `riverdriver/riversqlite/river_sqlite_driver.go`

SQLite's JSON syntax differs from Postgres. Use `json_extract` and `json_each`.

- [ ] **Step 1: Add the three queries (SQLite dialect)**

```sql
-- name: JobGetWorkflowTasks :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE json_extract(metadata, '$."river:workflow_id"') = @workflow_id
  AND (
    @task_names_count = 0
    OR json_extract(metadata, '$."river:workflow_task"') IN (sqlc.slice('task_names'))
  )
ORDER BY id;

-- name: JobCancelWorkflow :many
UPDATE /* TEMPLATE: schema */river_job
SET state        = 'cancelled',
    finalized_at = @now,
    errors       = json_insert(
                     COALESCE(errors, json('[]')),
                     '$[#]',
                     json_object('at', @now, 'attempt', attempt, 'error', @reason, 'trace', '')
                   )
WHERE json_extract(metadata, '$."river:workflow_id"') = @workflow_id
  AND finalized_at IS NULL
RETURNING *;

-- name: JobUpdateWorkflowReady :many
WITH candidates AS (
  SELECT j.id, j.metadata, j.scheduled_at
  FROM /* TEMPLATE: schema */river_job j
  WHERE j.state = 'pending'
    AND json_extract(j.metadata, '$."river:workflow_id"') IS NOT NULL
  ORDER BY j.id
  LIMIT @max
),
dep_states AS (
  SELECT
    c.id AS candidate_id,
    sib.state AS dep_state,
    json_extract(sib.metadata, '$."river:workflow_task"') AS dep_task
  FROM candidates c
  LEFT JOIN /* TEMPLATE: schema */river_job sib
    ON json_extract(sib.metadata, '$."river:workflow_id"') = json_extract(c.metadata, '$."river:workflow_id"')
   AND json_extract(sib.metadata, '$."river:workflow_task"') IN (
         SELECT value FROM json_each(json_extract(c.metadata, '$."river:workflow_deps"'))
       )
),
resolved AS (
  SELECT
    c.id,
    c.scheduled_at,
    c.metadata,
    -- All deps either completed, or in an ignored failure state:
    MIN(CASE
        WHEN d.dep_state IS NULL THEN 1
        WHEN d.dep_state = 'completed' THEN 1
        WHEN d.dep_state = 'cancelled' AND COALESCE(json_extract(c.metadata, '$."river:workflow_ignore_cancelled_deps"'), 0) = 1 THEN 1
        WHEN d.dep_state = 'discarded' AND COALESCE(json_extract(c.metadata, '$."river:workflow_ignore_discarded_deps"'), 0) = 1 THEN 1
        ELSE 0
    END) AS all_done,
    MAX(CASE WHEN d.dep_state = 'cancelled' AND COALESCE(json_extract(c.metadata, '$."river:workflow_ignore_cancelled_deps"'), 0) <> 1 THEN 1 ELSE 0 END) AS fail_cancelled,
    MAX(CASE WHEN d.dep_state = 'discarded' AND COALESCE(json_extract(c.metadata, '$."river:workflow_ignore_discarded_deps"'), 0) <> 1 THEN 1 ELSE 0 END) AS fail_discarded,
    SUM(CASE WHEN d.dep_state IS NOT NULL THEN 1 ELSE 0 END) AS dep_rows_found,
    (SELECT COUNT(*) FROM json_each(json_extract(c.metadata, '$."river:workflow_deps"'))) AS dep_rows_declared
  FROM candidates c
  LEFT JOIN dep_states d ON d.candidate_id = c.id
  GROUP BY c.id, c.scheduled_at, c.metadata
)
UPDATE /* TEMPLATE: schema */river_job
SET state = (SELECT CASE
    WHEN r.fail_cancelled = 1 OR r.fail_discarded = 1 THEN 'cancelled'
    WHEN r.dep_rows_found < r.dep_rows_declared
      AND COALESCE(json_extract(r.metadata, '$."river:workflow_ignore_deleted_deps"'), 0) <> 1 THEN 'cancelled'
    WHEN r.all_done = 1 AND r.dep_rows_found >= r.dep_rows_declared
      AND r.scheduled_at > @now THEN 'scheduled'
    WHEN r.all_done = 1 AND r.dep_rows_found >= r.dep_rows_declared THEN 'available'
    ELSE 'pending'
  END FROM resolved r WHERE r.id = /* TEMPLATE: schema */river_job.id),
finalized_at = (SELECT CASE
    WHEN r.fail_cancelled = 1 OR r.fail_discarded = 1 THEN @now
    WHEN r.dep_rows_found < r.dep_rows_declared
      AND COALESCE(json_extract(r.metadata, '$."river:workflow_ignore_deleted_deps"'), 0) <> 1 THEN @now
    ELSE finalized_at
  END FROM resolved r WHERE r.id = /* TEMPLATE: schema */river_job.id)
WHERE id IN (
  SELECT r.id FROM resolved r WHERE
    r.fail_cancelled = 1
    OR r.fail_discarded = 1
    OR (r.dep_rows_found < r.dep_rows_declared
        AND COALESCE(json_extract(r.metadata, '$."river:workflow_ignore_deleted_deps"'), 0) <> 1)
    OR (r.all_done = 1 AND r.dep_rows_found >= r.dep_rows_declared)
)
RETURNING *;
```

(If SQLite's CTE-in-UPDATE form rejects the above, fall back to: select IDs into a temp result, then run the UPDATE statement separately in the driver method. Either approach is acceptable as long as the behavior matches the exercises.)

- [ ] **Step 2: Regenerate**

Run: `make generate`

- [ ] **Step 3: Replace stub methods in `river_sqlite_driver.go`** (mirror Task 7).

- [ ] **Step 4: Run conformance tests**

Run: `cd riverdriver/riversqlite && go test ./... -run "TestDriver/JobUpdateWorkflowReady|TestDriver/JobGetWorkflowTasks|TestDriver/JobCancelWorkflow" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverdriver/riversqlite/
git commit -m "Implement workflow Executor methods for riversqlite"
```

---

## Phase 3 — Migration

### Task 10: Add `007_workflow_index` migration to all three drivers

**Files:**
- Create: `riverdriver/riverpgxv5/migration/main/007_workflow_index.up.sql`
- Create: `riverdriver/riverpgxv5/migration/main/007_workflow_index.down.sql`
- Create: `riverdriver/riverdatabasesql/migration/main/007_workflow_index.up.sql` (identical to pgxv5)
- Create: `riverdriver/riverdatabasesql/migration/main/007_workflow_index.down.sql`
- Create: `riverdriver/riversqlite/migration/main/007_workflow_index.up.sql`
- Create: `riverdriver/riversqlite/migration/main/007_workflow_index.down.sql`

- [ ] **Step 1: Write Postgres `up.sql` (used for pgxv5 + database/sql)**

```sql
CREATE INDEX IF NOT EXISTS river_job_workflow_id_idx
ON /* TEMPLATE: schema */river_job ((metadata->>'river:workflow_id'), state)
WHERE metadata ? 'river:workflow_id';
```

- [ ] **Step 2: Write Postgres `down.sql`**

```sql
DROP INDEX IF EXISTS river_job_workflow_id_idx;
```

(In Postgres, the index lives in the schema of the table — the schema-qualified drop only matters if migrations are run in a non-default schema; River's migration runner handles that uniformly. Confirm by reading `riverdriver/riverpgxv5/migration/main/006_bulk_unique.down.sql` for the prevailing style.)

- [ ] **Step 3: Write SQLite `up.sql`**

```sql
CREATE INDEX IF NOT EXISTS river_job_workflow_id_idx
ON /* TEMPLATE: schema */river_job (json_extract(metadata, '$."river:workflow_id"'), state)
WHERE json_extract(metadata, '$."river:workflow_id"') IS NOT NULL;
```

- [ ] **Step 4: Write SQLite `down.sql`**

```sql
DROP INDEX IF EXISTS river_job_workflow_id_idx;
```

- [ ] **Step 5: Run migrations end-to-end**

Run: `make test` (the existing migration tests apply 001-007 and assert the final schema state).
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add riverdriver/*/migration/main/007_workflow_index.*
git commit -m "Add workflow index migration to main line"
```

---

## Phase 4 — Core workflow API

### Task 11: Add `riverworkflow.ulid` helper

Workflow IDs must be globally unique and (ideally) lexicographically sortable. River has no existing ID library, and `AGENTS.md` discourages new dependencies. Implement a minimal Crockford-base32 ULID generator in-module.

**Files:**
- Create: `riverworkflow/internal/workflowid/workflowid.go`
- Create: `riverworkflow/internal/workflowid/workflowid_test.go`

- [ ] **Step 1: Write the failing test**

```go
package workflowid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	id := New()
	require.Len(t, id, 26)
	// Crockford base32 charset:
	for _, r := range id {
		require.True(t, isCrockford(r), "char %q not Crockford base32", r)
	}
}

func TestNew_UniqueAndSortable(t *testing.T) {
	t.Parallel()

	ids := make([]string, 1000)
	for i := range ids {
		ids[i] = New()
	}
	for i := 1; i < len(ids); i++ {
		require.NotEqual(t, ids[i-1], ids[i], "ULIDs must be unique")
	}
}

func isCrockford(r rune) bool {
	const c = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	for _, x := range c {
		if x == r {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run test, see it fail**

Run: `cd riverworkflow && go test ./internal/workflowid -v`
Expected: FAIL with `undefined: New`.

- [ ] **Step 3: Implement `workflowid.go`**

```go
// Package workflowid generates 26-character Crockford-base32 ULIDs for
// workflow IDs. The format mirrors github.com/oklog/ulid: 48-bit Unix-ms
// timestamp followed by 80 bits of randomness.
package workflowid

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	mu      sync.Mutex
	lastMs  uint64
	lastRnd [10]byte
)

// New returns a new ULID-shaped workflow ID.
func New() string {
	var raw [16]byte

	mu.Lock()
	defer mu.Unlock()

	ms := uint64(time.Now().UnixMilli()) //nolint:gosec
	binary.BigEndian.PutUint64(raw[0:8], ms<<16)
	if ms == lastMs {
		// Within the same millisecond, increment the previous random tail to
		// preserve monotonicity.
		incBytes(lastRnd[:])
	} else {
		if _, err := rand.Read(lastRnd[:]); err != nil {
			panic("workflowid: rand.Read: " + err.Error())
		}
		lastMs = ms
	}
	copy(raw[6:], lastRnd[:])

	return encode(raw)
}

func incBytes(b []byte) {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			return
		}
	}
}

func encode(raw [16]byte) string {
	out := make([]byte, 26)
	// 16 bytes = 128 bits; 26 base32 chars = 130 bits — pad with two leading zero bits.
	var v uint64
	bits := 0
	pos := 0
	for _, b := range raw {
		v = (v << 8) | uint64(b)
		bits += 8
		for bits >= 5 {
			bits -= 5
			out[pos] = crockfordAlphabet[(v>>bits)&0x1f]
			pos++
		}
	}
	if bits > 0 {
		out[pos] = crockfordAlphabet[(v<<(5-bits))&0x1f]
		pos++
	}
	if pos < 26 {
		// pad start
		for i := 26 - 1; i >= 26-pos; i-- {
			out[i] = out[i-(26-pos)]
		}
		for i := 0; i < 26-pos; i++ {
			out[i] = '0'
		}
	}
	return string(out)
}
```

- [ ] **Step 4: Run tests**

Run: `cd riverworkflow && go test ./internal/workflowid -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/internal/workflowid/
git commit -m "Add workflow ULID generator"
```

---

### Task 12: Define error sentinels

**Files:**
- Create: `riverworkflow/errors.go`
- Create: `riverworkflow/errors_test.go`

- [ ] **Step 1: Write the failing test**

```go
package riverworkflow

import (
	"errors"
	"testing"
)

func TestErrorSentinels(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		ErrWorkflowDepCycle,
		ErrWorkflowDepUnknown,
		ErrWorkflowEmpty,
		ErrWorkflowTaskNameDuplicate,
		ErrWorkflowTaskNameEmpty,
		ErrWorkflowTaskOutputMissing,
	} {
		if err == nil {
			t.Fatal("sentinel is nil")
		}
		if errors.Is(err, errors.New("other")) {
			t.Fatal("sentinel should not match arbitrary other error")
		}
	}
}
```

- [ ] **Step 2: Run test**

Run: `cd riverworkflow && go test ./... -run TestErrorSentinels -v`
Expected: FAIL.

- [ ] **Step 3: Implement `errors.go`**

```go
package riverworkflow

import "errors"

var (
	ErrWorkflowDepCycle          = errors.New("riverworkflow: dependency cycle detected")
	ErrWorkflowDepUnknown        = errors.New("riverworkflow: task references unknown dependency")
	ErrWorkflowEmpty             = errors.New("riverworkflow: workflow has no tasks")
	ErrWorkflowTaskNameDuplicate = errors.New("riverworkflow: duplicate task name")
	ErrWorkflowTaskNameEmpty     = errors.New("riverworkflow: task name is empty")
	ErrWorkflowTaskOutputMissing = errors.New("riverworkflow: task has no recorded output")
)
```

- [ ] **Step 4: Run tests**

Run: `cd riverworkflow && go test ./... -run TestErrorSentinels -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/errors.go riverworkflow/errors_test.go
git commit -m "Add riverworkflow sentinel errors"
```

---

### Task 13: Define workflow types (`WorkflowOpts`, `WorkflowTaskOpts`, `WorkflowTask`, `Workflow`)

**Files:**
- Create: `riverworkflow/workflow.go` (initial scaffolding)
- Create: `riverworkflow/workflow_test.go`

- [ ] **Step 1: Write the failing test**

```go
package riverworkflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type sortArgs struct {
	Strings []string `json:"strings"`
}

func (sortArgs) Kind() string { return "sort" }

func TestWorkflow_NewBasic(t *testing.T) {
	t.Parallel()

	w := newWorkflow[any](&WorkflowOpts{Name: "test"})
	require.NotEmpty(t, w.ID())
	require.Equal(t, "test", w.Name())
	require.Empty(t, w.tasks)
}
```

- [ ] **Step 2: Run, see it fail**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_NewBasic -v`
Expected: FAIL.

- [ ] **Step 3: Implement initial types and constructor**

```go
package riverworkflow

import (
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverworkflow/internal/workflowid"
)

type WorkflowOpts struct {
	ID                  string
	Name                string
	IgnoreCancelledDeps bool
	IgnoreDeletedDeps   bool
	IgnoreDiscardedDeps bool
}

type WorkflowTaskOpts struct {
	Deps                []string
	IgnoreCancelledDeps *bool
	IgnoreDeletedDeps   *bool
	IgnoreDiscardedDeps *bool
}

type WorkflowTask struct {
	Name string

	args      river.JobArgs
	deps      []string
	ignoreCancelled *bool
	ignoreDeleted   *bool
	ignoreDiscarded *bool
	jobOpts   *river.InsertOpts
}

type Workflow[TTx any] struct {
	id    string
	name  string
	opts  WorkflowOpts
	tasks []*WorkflowTask
}

func (w *Workflow[TTx]) ID() string   { return w.id }
func (w *Workflow[TTx]) Name() string { return w.name }

// newWorkflow is the package-internal constructor — Client.NewWorkflow is the
// public entry point and delegates here.
func newWorkflow[TTx any](opts *WorkflowOpts) *Workflow[TTx] {
	if opts == nil {
		opts = &WorkflowOpts{}
	}
	id := opts.ID
	if id == "" {
		id = workflowid.New()
	}
	return &Workflow[TTx]{
		id:   id,
		name: opts.Name,
		opts: *opts,
	}
}
```

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_NewBasic -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/workflow.go riverworkflow/workflow_test.go
git commit -m "Add Workflow type and constructor scaffold"
```

---

### Task 14: Implement `Workflow.Add`

**Files:**
- Modify: `riverworkflow/workflow.go`
- Modify: `riverworkflow/workflow_test.go`

- [ ] **Step 1: Write the failing test**

Append to `workflow_test.go`:

```go
func TestWorkflow_Add(t *testing.T) {
	t.Parallel()

	w := newWorkflow[any](nil)
	task := w.Add("a", sortArgs{Strings: []string{"x"}}, nil, nil)
	require.Equal(t, "a", task.Name)
	require.Len(t, w.tasks, 1)
	require.Same(t, task, w.tasks[0])

	taskB := w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
	require.Equal(t, []string{"a"}, taskB.deps)
	require.Len(t, w.tasks, 2)
}
```

- [ ] **Step 2: Run, fail**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_Add -v`
Expected: FAIL.

- [ ] **Step 3: Implement `Add`**

Append to `workflow.go`:

```go
func (w *Workflow[TTx]) Add(taskName string, args river.JobArgs, jobOpts *river.InsertOpts, taskOpts *WorkflowTaskOpts) *WorkflowTask {
	var (
		deps []string
		igC, igDc, igDe *bool
	)
	if taskOpts != nil {
		deps = append([]string(nil), taskOpts.Deps...)
		igC, igDc, igDe = taskOpts.IgnoreCancelledDeps, taskOpts.IgnoreDiscardedDeps, taskOpts.IgnoreDeletedDeps
	}

	task := &WorkflowTask{
		Name:            taskName,
		args:            args,
		deps:            deps,
		ignoreCancelled: igC,
		ignoreDeleted:   igDe,
		ignoreDiscarded: igDc,
		jobOpts:         jobOpts,
	}
	w.tasks = append(w.tasks, task)
	return task
}
```

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_Add -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/workflow.go riverworkflow/workflow_test.go
git commit -m "Implement Workflow.Add"
```

---

### Task 15: Implement DAG validation

**Files:**
- Modify: `riverworkflow/workflow.go`
- Modify: `riverworkflow/workflow_test.go`

- [ ] **Step 1: Write the failing test**

Append:

```go
func TestWorkflow_Validate(t *testing.T) {
	t.Parallel()

	t.Run("Empty", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil)
		require.ErrorIs(t, w.validate(), ErrWorkflowEmpty)
	})

	t.Run("EmptyName", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil)
		w.Add("", sortArgs{}, nil, nil)
		require.ErrorIs(t, w.validate(), ErrWorkflowTaskNameEmpty)
	})

	t.Run("DuplicateName", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil)
		w.Add("a", sortArgs{}, nil, nil)
		w.Add("a", sortArgs{}, nil, nil)
		require.ErrorIs(t, w.validate(), ErrWorkflowTaskNameDuplicate)
	})

	t.Run("UnknownDep", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil)
		w.Add("a", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"ghost"}})
		require.ErrorIs(t, w.validate(), ErrWorkflowDepUnknown)
	})

	t.Run("Cycle", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil)
		w.Add("a", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"b"}})
		w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
		require.ErrorIs(t, w.validate(), ErrWorkflowDepCycle)
	})

	t.Run("ValidDiamond", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil)
		w.Add("a", sortArgs{}, nil, nil)
		w.Add("b1", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
		w.Add("b2", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
		w.Add("c", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"b1", "b2"}})
		require.NoError(t, w.validate())
	})
}
```

- [ ] **Step 2: Run, fail**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_Validate -v`
Expected: FAIL.

- [ ] **Step 3: Implement `validate` (Kahn's algorithm)**

Append to `workflow.go`:

```go
import "fmt"

func (w *Workflow[TTx]) validate() error {
	if len(w.tasks) == 0 {
		return ErrWorkflowEmpty
	}

	byName := make(map[string]*WorkflowTask, len(w.tasks))
	for _, t := range w.tasks {
		if t.Name == "" {
			return ErrWorkflowTaskNameEmpty
		}
		if _, dup := byName[t.Name]; dup {
			return fmt.Errorf("%w: %q", ErrWorkflowTaskNameDuplicate, t.Name)
		}
		byName[t.Name] = t
	}

	// Validate deps reference known tasks.
	indegree := make(map[string]int, len(w.tasks))
	adj := make(map[string][]string, len(w.tasks))
	for _, t := range w.tasks {
		indegree[t.Name] = 0
		adj[t.Name] = nil
	}
	for _, t := range w.tasks {
		for _, dep := range t.deps {
			if _, ok := byName[dep]; !ok {
				return fmt.Errorf("%w: task %q depends on %q", ErrWorkflowDepUnknown, t.Name, dep)
			}
			indegree[t.Name]++
			adj[dep] = append(adj[dep], t.Name)
		}
	}

	// Kahn's topological sort.
	queue := make([]string, 0, len(w.tasks))
	for name, d := range indegree {
		if d == 0 {
			queue = append(queue, name)
		}
	}
	processed := 0
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		processed++
		for _, next := range adj[name] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if processed != len(w.tasks) {
		return ErrWorkflowDepCycle
	}
	return nil
}
```

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_Validate -v`
Expected: PASS for all subtests.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/workflow.go riverworkflow/workflow_test.go
git commit -m "Implement workflow DAG validation"
```

---

### Task 16: Implement `Workflow.Prepare` / `PrepareTx`

Prepare renders each task into `river.InsertManyParams`, injecting workflow metadata and forcing state=Pending for tasks with deps.

**Files:**
- Modify: `riverworkflow/workflow.go`
- Modify: `riverworkflow/workflow_test.go`

- [ ] **Step 1: Write the failing test**

Append:

```go
func TestWorkflow_Prepare(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	w := newWorkflow[any](&WorkflowOpts{Name: "billing"})
	w.Add("a", sortArgs{}, nil, nil)
	w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})

	res, err := w.Prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, w.ID(), res.WorkflowID)
	require.Len(t, res.Jobs, 2)

	// First task: no deps → state defaulted by river; we inject metadata only.
	require.NotNil(t, res.Jobs[0].InsertOpts)
	require.Equal(t, w.ID(), res.Jobs[0].InsertOpts.Metadata[rivercommon.MetadataKeyWorkflowID])
	require.Equal(t, "billing", res.Jobs[0].InsertOpts.Metadata[rivercommon.MetadataKeyWorkflowName])
	require.Equal(t, "a", res.Jobs[0].InsertOpts.Metadata[rivercommon.MetadataKeyWorkflowTask])
	require.Empty(t, res.Jobs[0].InsertOpts.Metadata[rivercommon.MetadataKeyWorkflowDeps])
	require.Equal(t, rivertype.JobStateAvailable, res.Jobs[0].InsertOpts.State)

	// Second task: has deps → state Pending.
	require.Equal(t, "b", res.Jobs[1].InsertOpts.Metadata[rivercommon.MetadataKeyWorkflowTask])
	require.Equal(t, []string{"a"}, res.Jobs[1].InsertOpts.Metadata[rivercommon.MetadataKeyWorkflowDeps])
	require.Equal(t, rivertype.JobStatePending, res.Jobs[1].InsertOpts.State)
}
```

(Imports needed: `context`, `github.com/riverqueue/river/internal/rivercommon`, `github.com/riverqueue/river/rivertype`.)

- [ ] **Step 2: Run, fail**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_Prepare -v`
Expected: FAIL.

- [ ] **Step 3: Implement `Prepare` + `PrepareTx`**

Append to `workflow.go`:

```go
import (
	"context"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/rivertype"
)

type WorkflowPrepareResult struct {
	WorkflowID string
	Jobs       []river.InsertManyParams
}

func (w *Workflow[TTx]) Prepare(_ context.Context) (*WorkflowPrepareResult, error) {
	return w.prepare()
}

func (w *Workflow[TTx]) PrepareTx(_ context.Context, _ TTx) (*WorkflowPrepareResult, error) {
	// Prepare doesn't touch the database; the Tx form exists for API symmetry
	// with riverpro and to leave room for future DB-aware preparation.
	return w.prepare()
}

func (w *Workflow[TTx]) prepare() (*WorkflowPrepareResult, error) {
	if err := w.validate(); err != nil {
		return nil, err
	}

	jobs := make([]river.InsertManyParams, 0, len(w.tasks))
	for _, t := range w.tasks {
		base := t.jobOpts
		if base == nil {
			base = &river.InsertOpts{}
		}
		opts := *base
		if opts.Metadata == nil {
			opts.Metadata = map[string]any{}
		}
		opts.Metadata[rivercommon.MetadataKeyWorkflowID] = w.id
		if w.name != "" {
			opts.Metadata[rivercommon.MetadataKeyWorkflowName] = w.name
		}
		opts.Metadata[rivercommon.MetadataKeyWorkflowTask] = t.Name
		if len(t.deps) > 0 {
			opts.Metadata[rivercommon.MetadataKeyWorkflowDeps] = t.deps
			opts.State = rivertype.JobStatePending
		}

		applyIgnore := func(taskFlag *bool, workflowFlag bool, key string) {
			switch {
			case taskFlag != nil:
				if *taskFlag {
					opts.Metadata[key] = true
				}
			case workflowFlag:
				opts.Metadata[key] = true
			}
		}
		applyIgnore(t.ignoreCancelled, w.opts.IgnoreCancelledDeps, rivercommon.MetadataKeyWorkflowIgnoreCancelledDeps)
		applyIgnore(t.ignoreDiscarded, w.opts.IgnoreDiscardedDeps, rivercommon.MetadataKeyWorkflowIgnoreDiscardedDeps)
		applyIgnore(t.ignoreDeleted, w.opts.IgnoreDeletedDeps, rivercommon.MetadataKeyWorkflowIgnoreDeletedDeps)

		jobs = append(jobs, river.InsertManyParams{
			Args:       t.args,
			InsertOpts: &opts,
		})
	}
	return &WorkflowPrepareResult{WorkflowID: w.id, Jobs: jobs}, nil
}
```

Note: `river.InsertOpts.State` may not be exported today — if it isn't, this task expands to add a public `State` field on `InsertOpts` (one extra commit before Prepare lands). Check `insert_opts.go` first; if absent, add:

```go
// In github.com/riverqueue/river/insert_opts.go:
// Add to InsertOpts struct:
// State rivertype.JobState
```

…and thread it through `JobInsertFastMany` so non-empty `State` overrides the default Available/Scheduled choice. Mirror how existing fields like `ScheduledAt` are threaded. Cover with a unit test in `insert_opts_test.go`.

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./... -run TestWorkflow_Prepare -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/workflow.go riverworkflow/workflow_test.go
# plus, if applicable:
git add insert_opts.go insert_opts_test.go client.go
git commit -m "Implement Workflow.Prepare/PrepareTx with metadata injection"
```

---

## Phase 5 — Client wrapper

### Task 17: Define `Config` + `WorkflowSchedulerConfig`

**Files:**
- Create: `riverworkflow/config.go`
- Create: `riverworkflow/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
package riverworkflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.applyDefaults()
	require.Equal(t, 5*time.Second, cfg.WorkflowScheduler.Interval)
	require.Equal(t, 1000, cfg.WorkflowScheduler.BatchSize)
}
```

- [ ] **Step 2: Run, fail**

Run: `cd riverworkflow && go test ./... -run TestConfig_Defaults -v`
Expected: FAIL.

- [ ] **Step 3: Implement `config.go`**

```go
package riverworkflow

import (
	"time"

	"github.com/riverqueue/river"
)

type Config struct {
	river.Config
	WorkflowScheduler WorkflowSchedulerConfig
}

type WorkflowSchedulerConfig struct {
	BatchSize int
	Interval  time.Duration
}

func (c *Config) applyDefaults() {
	if c.WorkflowScheduler.Interval <= 0 {
		c.WorkflowScheduler.Interval = 5 * time.Second
	}
	if c.WorkflowScheduler.BatchSize <= 0 {
		c.WorkflowScheduler.BatchSize = 1000
	}
}
```

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./... -run TestConfig_Defaults -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/config.go riverworkflow/config_test.go
git commit -m "Add riverworkflow Config type"
```

---

### Task 18: Implement `Client` wrapper + `NewClient` + `NewWorkflow`

The client wraps the user's driver in a `workflowDriverPlugin` so that River's `pilotPlugin` mechanism injects the `WorkflowScheduler` as a leader-elected maintenance service. The scheduler itself is built in Task 19; this task just stubs an empty `PluginMaintenanceServices()`.

**Files:**
- Create: `riverworkflow/client.go`
- Create: `riverworkflow/driver_plugin.go`
- Create: `riverworkflow/client_test.go`

- [ ] **Step 1: Implement `driver_plugin.go`**

```go
package riverworkflow

import (
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
)

// workflowDriverPlugin wraps a riverdriver.Driver to inject a custom pilot via
// the River driverPlugin interface. The pilot itself wraps the driver's
// existing pilot (or StandardPilot) and adds the workflow scheduler as a
// leader-elected maintenance service.
type workflowDriverPlugin[TTx any] struct {
	riverdriver.Driver[TTx]
	pilot *workflowPilot
}

func (p *workflowDriverPlugin[TTx]) PluginInit(archetype *baseservice.Archetype) {
	p.pilot.archetype = archetype
}

func (p *workflowDriverPlugin[TTx]) PluginPilot() riverpilot.Pilot {
	return p.pilot
}

type workflowPilot struct {
	*riverpilot.StandardPilot

	archetype *baseservice.Archetype
	services  []startstop.Service
}

func (p *workflowPilot) PluginMaintenanceServices() []startstop.Service {
	return p.services
}

func (p *workflowPilot) PluginServices() []startstop.Service { return nil }
```

- [ ] **Step 2: Implement `client.go`**

```go
package riverworkflow

import (
	"context"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
)

type Client[TTx any] struct {
	*river.Client[TTx]

	driver riverdriver.Driver[TTx]
	config *Config
}

func NewClient[TTx any](driver riverdriver.Driver[TTx], config *Config) (*Client[TTx], error) {
	if config == nil {
		config = &Config{}
	}
	config.applyDefaults()

	pilot := &workflowPilot{StandardPilot: &riverpilot.StandardPilot{}}
	plugin := &workflowDriverPlugin[TTx]{Driver: driver, pilot: pilot}

	riverClient, err := river.NewClient(plugin, &config.Config)
	if err != nil {
		return nil, err
	}

	wc := &Client[TTx]{
		Client: riverClient,
		driver: driver,
		config: config,
	}
	pilot.services = wc.maintenanceServices()
	return wc, nil
}

func (c *Client[TTx]) maintenanceServices() []startstop.Service {
	// Populated in Task 19 with the workflow scheduler.
	return nil
}

func (c *Client[TTx]) NewWorkflow(opts *WorkflowOpts) *Workflow[TTx] {
	return newWorkflow[TTx](opts)
}

func (c *Client[TTx]) ctx(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
```

(Imports — add `"github.com/riverqueue/river/rivershared/riverpilot"` and `"github.com/riverqueue/river/rivershared/startstop"` in client.go too.)

- [ ] **Step 3: Write the smoke test**

```go
package riverworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdbtest"
)

func TestClient_NewWorkflow(t *testing.T) {
	t.Parallel()

	dbPool := riverdbtest.TestDB(t, nil) // however the helper is named — adjust per riverdbtest API
	_ = dbPool

	workers := river.NewWorkers()
	river.AddWorker(workers, &testNoopWorker{})

	c, err := NewClient(riverpgxv5.New(nil), &Config{
		Config: river.Config{
			Workers: workers,
		},
	})
	require.NoError(t, err)

	w := c.NewWorkflow(&WorkflowOpts{Name: "smoke"})
	require.Equal(t, "smoke", w.Name())
}

type testNoopWorker struct{ river.WorkerDefaults[sortArgs] }

func (w *testNoopWorker) Work(_ context.Context, _ *river.Job[sortArgs]) error { return nil }
```

If `riverdbtest.TestDB` differs from the example, look at `riverdbtest/riverdbtest.go` for the actual public API and adjust.

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./... -run TestClient_NewWorkflow -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/client.go riverworkflow/driver_plugin.go riverworkflow/client_test.go
git commit -m "Add riverworkflow Client wrapper and NewWorkflow entry point"
```

---

## Phase 6 — WorkflowScheduler service

### Task 19: Implement `WorkflowScheduler`

**Files:**
- Create: `riverworkflow/internal/workflowscheduler/workflow_scheduler.go`
- Create: `riverworkflow/internal/workflowscheduler/workflow_scheduler_test.go`

Pattern after `internal/maintenance/job_scheduler.go`. This is a `startstop.Service` with a periodic tick.

- [ ] **Step 1: Write the failing test**

```go
package workflowscheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

func TestWorkflowScheduler_RunOnce(t *testing.T) {
	t.Parallel()

	exec := &fakeExec{}
	s := New(baseservice.TestArchetype(testutil.WrapTestingT(t)), &Config{
		Interval:  10 * time.Millisecond,
		BatchSize: 100,
		Schema:    "public",
	}, exec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, s.Start(ctx))
	defer s.Stop()

	s.TestSignals.ScheduledBatch.WaitOrTimeout()
	require.GreaterOrEqual(t, exec.calls, 1)
}
```

Add a small `fakeExec` to satisfy `riverdriver.Executor` minimally — only `JobUpdateWorkflowReady` is exercised; embed `riverdriver.Executor` interface and panic on others, or use a `mockExec` already in `riverinternaltest`. Inspect that package for the existing pattern; this is OK to copy/adapt.

- [ ] **Step 2: Run, fail**

Run: `cd riverworkflow && go test ./internal/workflowscheduler -v`
Expected: FAIL.

- [ ] **Step 3: Implement the scheduler**

```go
package workflowscheduler

import (
	"cmp"
	"context"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

const IntervalDefault = 5 * time.Second

type Config struct {
	BatchSize int
	Interval  time.Duration
	Schema    string
}

type TestSignals struct {
	ScheduledBatch testsignal.TestSignal[struct{}]
}

func (ts *TestSignals) Init(tb testutil.TestingTB) {
	ts.ScheduledBatch.Init(tb)
}

type WorkflowScheduler struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	TestSignals TestSignals

	config *Config
	exec   riverdriver.Executor
}

func New(archetype *baseservice.Archetype, config *Config, exec riverdriver.Executor) *WorkflowScheduler {
	return baseservice.Init(archetype, &WorkflowScheduler{
		config: &Config{
			BatchSize: cmp.Or(config.BatchSize, 1000),
			Interval:  cmp.Or(config.Interval, IntervalDefault),
			Schema:    config.Schema,
		},
		exec: exec,
	})
}

func (s *WorkflowScheduler) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	s.StaggerStart(ctx)

	go func() {
		started()
		defer stopped()

		ticker := time.NewTicker(s.config.Interval)
		defer ticker.Stop()

		for {
			if err := s.runOnce(ctx); err != nil && !serviceutil.IsContextCancelledError(err) {
				s.Logger.ErrorContext(ctx, s.Name+": run failed", "err", err.Error())
			}
			s.TestSignals.ScheduledBatch.Signal(struct{}{})

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return nil
}

func (s *WorkflowScheduler) runOnce(ctx context.Context) error {
	for {
		rows, err := s.exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
			Schema: s.config.Schema,
			Max:    s.config.BatchSize,
			Now:    time.Now(),
		})
		if err != nil {
			return err
		}
		if len(rows) < s.config.BatchSize {
			return nil
		}
	}
}
```

(Confirm helper names — `serviceutil.IsContextCancelledError` is illustrative; River's pattern is in `job_scheduler.go`. Mirror exactly.)

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./internal/workflowscheduler -v`
Expected: PASS.

- [ ] **Step 5: Wire scheduler into the client (Task 18 stub)**

Open `riverworkflow/client.go` and replace the placeholder:

```go
func (c *Client[TTx]) maintenanceServices() []startstop.Service {
	return []startstop.Service{
		workflowscheduler.New(c.Client.Archetype(), &workflowscheduler.Config{
			BatchSize: c.config.WorkflowScheduler.BatchSize,
			Interval:  c.config.WorkflowScheduler.Interval,
			Schema:    c.config.Schema,
		}, c.driver.GetExecutor()),
	}
}
```

(If `*river.Client` doesn't expose `Archetype()` — grep `func (c \*Client.*Archetype` — add a thin accessor or capture the archetype during `PluginInit`. The driver plugin already receives the archetype.)

- [ ] **Step 6: Build & commit**

Run: `cd riverworkflow && go build ./... && go test ./...`
Expected: pass.

```bash
git add riverworkflow/internal/workflowscheduler/ riverworkflow/client.go
git commit -m "Add WorkflowScheduler maintenance service"
```

---

## Phase 7 — Cancellation

### Task 20: Implement `WorkflowCancel` + `WorkflowCancelTx`

**Files:**
- Modify: `riverworkflow/client.go`
- Create: `riverworkflow/client_cancel_test.go`

- [ ] **Step 1: Write the failing integration test**

```go
package riverworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient_WorkflowCancel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bundle := setupIntegration(t) // see helper in workflow_integration_test.go

	w := bundle.client.NewWorkflow(&WorkflowOpts{})
	w.Add("a", sortArgs{}, nil, nil)
	w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})

	prep, err := w.Prepare(ctx)
	require.NoError(t, err)
	_, err = bundle.client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	res, err := bundle.client.WorkflowCancel(ctx, prep.WorkflowID)
	require.NoError(t, err)
	require.Len(t, res.CancelledJobs, 2)
}
```

Create `riverworkflow/workflow_integration_test.go` with a `setupIntegration` helper that boots a `riverdbtest`-backed client and returns `{client *Client[pgx.Tx], schema string}`.

- [ ] **Step 2: Run, fail**

Run: `cd riverworkflow && go test ./... -run TestClient_WorkflowCancel -v`
Expected: FAIL.

- [ ] **Step 3: Implement the methods**

Append to `riverworkflow/client.go`:

```go
type WorkflowCancelResult struct {
	CancelledJobs []*rivertype.JobRow
}

func (c *Client[TTx]) WorkflowCancel(ctx context.Context, workflowID string) (*WorkflowCancelResult, error) {
	rows, err := c.driver.GetExecutor().JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
		Schema:     c.config.Schema,
		WorkflowID: workflowID,
		Now:        time.Now(),
		Reason:     "workflow cancelled by client",
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowCancelResult{CancelledJobs: rows}, nil
}

func (c *Client[TTx]) WorkflowCancelTx(ctx context.Context, tx TTx, workflowID string) (*WorkflowCancelResult, error) {
	execTx, err := c.driver.UnwrapExecutor(tx)
	if err != nil {
		return nil, err
	}
	rows, err := execTx.JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
		Schema:     c.config.Schema,
		WorkflowID: workflowID,
		Now:        time.Now(),
		Reason:     "workflow cancelled by client",
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowCancelResult{CancelledJobs: rows}, nil
}
```

(Confirm `Driver.UnwrapExecutor` exists; grep the driver interface. If not, use whatever helper extracts a transactional executor — `riverdrivertest` and `client.go` both do this for similar transactional methods.)

- [ ] **Step 4: Run**

Run: `cd riverworkflow && go test ./... -run TestClient_WorkflowCancel -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add riverworkflow/client.go riverworkflow/client_cancel_test.go riverworkflow/workflow_integration_test.go
git commit -m "Implement WorkflowCancel and WorkflowCancelTx"
```

---

## Phase 8 — Loading & dynamic add

### Task 21: Implement `WorkflowTasks`, `LoadDeps`, `LoadAll`, `WorkflowFromExisting`

**Files:**
- Create: `riverworkflow/workflow_tasks.go`
- Create: `riverworkflow/workflow_tasks_test.go`
- Modify: `riverworkflow/workflow.go`
- Modify: `riverworkflow/client.go`

- [ ] **Step 1: Write the failing tests**

```go
package riverworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflow_LoadAll(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bundle := setupIntegration(t)

	w := bundle.client.NewWorkflow(&WorkflowOpts{})
	w.Add("a", sortArgs{}, nil, nil)
	w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
	prep, err := w.Prepare(ctx)
	require.NoError(t, err)
	_, err = bundle.client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	tasks, err := w.LoadAll(ctx)
	require.NoError(t, err)
	require.NotNil(t, tasks)

	a, err := tasks.Get("a")
	require.NoError(t, err)
	require.Equal(t, "a", a.Metadata[rivercommon.MetadataKeyWorkflowTask])
}

func TestWorkflowTasks_Output_Missing(t *testing.T) {
	t.Parallel()

	tasks := &WorkflowTasks{byName: map[string]*rivertype.JobRow{
		"a": {Metadata: []byte(`{}`)},
	}}
	var out struct{ V int }
	err := tasks.Output("a", &out)
	require.ErrorIs(t, err, ErrWorkflowTaskOutputMissing)
}

func TestClient_WorkflowFromExisting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bundle := setupIntegration(t)

	w := bundle.client.NewWorkflow(&WorkflowOpts{})
	w.Add("a", sortArgs{}, nil, nil)
	prep, err := w.Prepare(ctx)
	require.NoError(t, err)
	rows, err := bundle.client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	w2, err := bundle.client.WorkflowFromExisting(rows[0].Job, nil)
	require.NoError(t, err)
	require.Equal(t, prep.WorkflowID, w2.ID())
}
```

- [ ] **Step 2: Run, fail**

Run: `cd riverworkflow && go test ./... -run "TestWorkflow_LoadAll|TestWorkflowTasks_Output_Missing|TestClient_WorkflowFromExisting" -v`
Expected: FAIL.

- [ ] **Step 3: Implement `workflow_tasks.go`**

```go
package riverworkflow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

type LoadDepsOpts struct {
	Recursive bool
}

type WorkflowTasks struct {
	byName map[string]*rivertype.JobRow
}

func (wt *WorkflowTasks) Get(taskName string) (*rivertype.JobRow, error) {
	row, ok := wt.byName[taskName]
	if !ok {
		return nil, fmt.Errorf("riverworkflow: task %q not found", taskName)
	}
	return row, nil
}

func (wt *WorkflowTasks) Output(taskName string, out any) error {
	row, err := wt.Get(taskName)
	if err != nil {
		return err
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(row.Metadata, &meta); err != nil {
		return fmt.Errorf("riverworkflow: parse metadata: %w", err)
	}
	raw, ok := meta[rivertype.MetadataKeyOutput]
	if !ok {
		return ErrWorkflowTaskOutputMissing
	}
	return json.Unmarshal(raw, out)
}

func (w *Workflow[TTx]) LoadAll(ctx context.Context) (*WorkflowTasks, error) {
	return w.loadAllOnExec(ctx, w.exec)
}

func (w *Workflow[TTx]) LoadAllTx(ctx context.Context, tx TTx) (*WorkflowTasks, error) {
	exec, err := w.driver.UnwrapExecutor(tx)
	if err != nil {
		return nil, err
	}
	return w.loadAllOnExec(ctx, exec)
}

func (w *Workflow[TTx]) loadAllOnExec(ctx context.Context, exec riverdriver.Executor) (*WorkflowTasks, error) {
	rows, err := exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
		Schema:     w.schema,
		WorkflowID: w.id,
	})
	if err != nil {
		return nil, err
	}
	return tasksFromRows(rows), nil
}

func (w *Workflow[TTx]) LoadDeps(ctx context.Context, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error) {
	return w.loadDepsOnExec(ctx, w.exec, taskName, opts)
}

func (w *Workflow[TTx]) LoadDepsTx(ctx context.Context, tx TTx, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error) {
	exec, err := w.driver.UnwrapExecutor(tx)
	if err != nil {
		return nil, err
	}
	return w.loadDepsOnExec(ctx, exec, taskName, opts)
}

func (w *Workflow[TTx]) loadDepsOnExec(ctx context.Context, exec riverdriver.Executor, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error) {
	all, err := w.loadAllOnExec(ctx, exec)
	if err != nil {
		return nil, err
	}
	return walkDeps(all, taskName, opts != nil && opts.Recursive), nil
}

func tasksFromRows(rows []*rivertype.JobRow) *WorkflowTasks {
	out := &WorkflowTasks{byName: make(map[string]*rivertype.JobRow, len(rows))}
	for _, r := range rows {
		var meta map[string]json.RawMessage
		_ = json.Unmarshal(r.Metadata, &meta)
		var name string
		if raw, ok := meta[rivercommon.MetadataKeyWorkflowTask]; ok {
			_ = json.Unmarshal(raw, &name)
		}
		if name != "" {
			out.byName[name] = r
		}
	}
	return out
}

func walkDeps(all *WorkflowTasks, taskName string, recursive bool) *WorkflowTasks {
	out := &WorkflowTasks{byName: map[string]*rivertype.JobRow{}}
	seen := map[string]bool{}
	var visit func(name string)
	visit = func(name string) {
		row, ok := all.byName[name]
		if !ok {
			return
		}
		var meta map[string]json.RawMessage
		_ = json.Unmarshal(row.Metadata, &meta)
		var deps []string
		if raw, ok := meta[rivercommon.MetadataKeyWorkflowDeps]; ok {
			_ = json.Unmarshal(raw, &deps)
		}
		for _, dep := range deps {
			if seen[dep] {
				continue
			}
			seen[dep] = true
			if depRow, ok := all.byName[dep]; ok {
				out.byName[dep] = depRow
			}
			if recursive {
				visit(dep)
			}
		}
	}
	visit(taskName)
	return out
}
```

- [ ] **Step 4: Extend `Workflow` to hold a driver and schema reference**

Modify `Workflow[TTx]` in `workflow.go` to embed driver + schema, set by `newWorkflow`. `newWorkflow` is now called from `Client.NewWorkflow`, so pass them in:

```go
type Workflow[TTx any] struct {
	// ... existing
	driver riverdriver.Driver[TTx]
	exec   riverdriver.Executor
	schema string
}

func newWorkflow[TTx any](opts *WorkflowOpts, driver riverdriver.Driver[TTx], schema string) *Workflow[TTx] {
	// existing logic ...
	w.driver = driver
	if driver != nil {
		w.exec = driver.GetExecutor()
	}
	w.schema = schema
	return w
}
```

Update the unit test in Task 13 to pass `nil` driver where it's not needed.

Update `Client.NewWorkflow`:

```go
func (c *Client[TTx]) NewWorkflow(opts *WorkflowOpts) *Workflow[TTx] {
	return newWorkflow[TTx](opts, c.driver, c.config.Schema)
}
```

- [ ] **Step 5: Implement `Client.WorkflowFromExisting`**

```go
func (c *Client[TTx]) WorkflowFromExisting(jobRow *rivertype.JobRow, opts *WorkflowOpts) (*Workflow[TTx], error) {
	if jobRow == nil {
		return nil, fmt.Errorf("riverworkflow: jobRow is nil")
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(jobRow.Metadata, &meta); err != nil {
		return nil, fmt.Errorf("riverworkflow: parse metadata: %w", err)
	}
	raw, ok := meta[rivercommon.MetadataKeyWorkflowID]
	if !ok {
		return nil, fmt.Errorf("riverworkflow: job has no workflow metadata")
	}
	var id string
	if err := json.Unmarshal(raw, &id); err != nil {
		return nil, fmt.Errorf("riverworkflow: parse workflow id: %w", err)
	}

	if opts == nil {
		opts = &WorkflowOpts{}
	}
	opts.ID = id
	return newWorkflow[TTx](opts, c.driver, c.config.Schema), nil
}
```

- [ ] **Step 6: Run all riverworkflow tests**

Run: `cd riverworkflow && go test ./... -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add riverworkflow/workflow_tasks.go riverworkflow/workflow_tasks_test.go riverworkflow/workflow.go riverworkflow/client.go
git commit -m "Implement WorkflowTasks, LoadDeps, LoadAll, and WorkflowFromExisting"
```

---

## Phase 9 — Examples & documentation

### Task 22: Add fan-out / fan-in example

**Files:**
- Create: `riverworkflow/example_workflow_test.go`

- [ ] **Step 1: Add the runnable example**

```go
package riverworkflow_test

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverworkflow"
)

type sortArgs struct {
	Strings []string `json:"strings"`
}

func (sortArgs) Kind() string { return "sort" }

type sortWorker struct {
	river.WorkerDefaults[sortArgs]
}

func (w *sortWorker) Work(_ context.Context, job *river.Job[sortArgs]) error {
	fmt.Printf("Worked task: %s\n", job.Metadata)
	return nil
}

// Example_workflowFanOutFanIn demonstrates a workflow with a single root task
// "a", two fan-out children "b1" and "b2", and a fan-in tail task "c".
func Example_workflowFanOutFanIn() {
	ctx := context.Background()

	dbPool := mustExamplePool() // wired up by surrounding infrastructure; see example_*_test.go in repo root

	workers := river.NewWorkers()
	river.AddWorker(workers, &sortWorker{})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
			Workers: workers,
		},
	})
	if err != nil {
		panic(err)
	}

	w := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "fan-out demo"})
	taskA := w.Add("a", sortArgs{}, nil, nil)
	taskB1 := w.Add("b1", sortArgs{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{taskA.Name}})
	taskB2 := w.Add("b2", sortArgs{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{taskA.Name}})
	w.Add("c", sortArgs{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{taskB1.Name, taskB2.Name}})

	prep, err := w.Prepare(ctx)
	if err != nil {
		panic(err)
	}

	var _ pgx.Tx
	if _, err := client.InsertMany(ctx, prep.Jobs); err != nil {
		panic(err)
	}

	fmt.Println("workflow inserted")
	// Output: workflow inserted
}

func mustExamplePool() any { panic("wire to existing example DB helper") } // replaced when wiring into repo
```

(Read `example_insert_and_work_test.go` at the repo root for the actual `mustExamplePool` / `riverdbtest` helper the existing examples use — copy that pattern verbatim.)

- [ ] **Step 2: Run the example**

Run: `cd riverworkflow && go test ./... -run Example_workflowFanOutFanIn -v`
Expected: PASS (Output matches).

- [ ] **Step 3: Commit**

```bash
git add riverworkflow/example_workflow_test.go
git commit -m "Add fan-out / fan-in workflow example"
```

---

### Task 23: Add cancel + LoadDeps examples

**Files:**
- Create: `riverworkflow/example_workflow_cancel_test.go`
- Create: `riverworkflow/example_workflow_load_deps_test.go`

- [ ] **Step 1: Write the cancel example**

Mirror Task 22's setup, then:

```go
res, err := client.WorkflowCancel(ctx, prep.WorkflowID)
if err != nil {
    panic(err)
}
fmt.Printf("cancelled %d tasks\n", len(res.CancelledJobs))
// Output: cancelled 4
```

- [ ] **Step 2: Write the LoadDeps example**

Inside a worker, call `riverworkflow.ClientFromContext(ctx)` — or for v1, accept the client passed in via closure. Then:

```go
tasks, err := w.LoadDeps(ctx, "c", &riverworkflow.LoadDepsOpts{Recursive: true})
if err != nil {
    return err
}
b1, _ := tasks.Get("b1")
fmt.Printf("b1 state: %s\n", b1.State)
// Output: b1 state: completed
```

(If `ClientFromContext` doesn't exist yet, accept this is awkward for v1 and document it in the example as a limitation. Future task: add a `riverworkflow.ClientFromContext` mirroring `river.ClientFromContext`.)

- [ ] **Step 3: Run both examples**

Run: `cd riverworkflow && go test ./... -run "Example_workflowCancel|Example_workflowLoadDeps" -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add riverworkflow/example_workflow_cancel_test.go riverworkflow/example_workflow_load_deps_test.go
git commit -m "Add workflow cancel and LoadDeps examples"
```

---

### Task 24: Add `riverworkflow/README.md`

**Files:**
- Create: `riverworkflow/README.md`

- [ ] **Step 1: Write the README**

```markdown
# riverworkflow

Workflow DAGs for [River](https://github.com/riverqueue/river).

`riverworkflow.Client` wraps `*river.Client` and adds methods to build, insert,
inspect, and cancel multi-step workflows where tasks have declared dependencies
on each other.

## Quickstart

See `example_workflow_test.go` for a runnable fan-out / fan-in example.

## Migration index

Add the `007_workflow_index` migration to your database via the standard
`rivermigrate` tooling before deploying workflows in production.
```

- [ ] **Step 2: Commit**

```bash
git add riverworkflow/README.md
git commit -m "Add riverworkflow README"
```

---

## Phase 10 — Final verification

### Task 25: `make test` clean across the workspace

- [ ] **Step 1: Run the full test suite**

Run: `make test`
Expected: PASS.

- [ ] **Step 2: If any failures, fix them inline; commit each fix as a separate commit.**

### Task 26: `make test/race` clean

- [ ] **Step 1: Run the race-detector suite**

Run: `make test/race`
Expected: PASS.

### Task 27: `make lint` clean

- [ ] **Step 1: Run lint**

Run: `make lint`
Expected: PASS.

- [ ] **Step 2: Fix any reported issues. Common gotchas:**
  - Import order — run `gci` per AGENTS.md sections.
  - Alphabetical ordering of struct fields / methods.
  - Exported symbols missing godoc comments.
  - `require` not `assert` in tests.

### Task 28: Update CHANGELOG.md

- [ ] **Step 1: Append a `## [Unreleased]` entry**

```markdown
### Added
- New `riverworkflow` submodule providing fan-out / fan-in workflow DAGs with
  a leader-elected `WorkflowScheduler` maintenance service. Mirrors the
  riverpro `Workflow` API. See `riverworkflow/README.md`.
- New driver methods on `riverdriver.Executor`: `JobCancelWorkflow`,
  `JobGetWorkflowTasks`, `JobUpdateWorkflowReady`, implemented across
  `riverpgxv5`, `riverdatabasesql`, and `riversqlite`.
- New migration `007_workflow_index` adds an index on
  `metadata->>'river:workflow_id'` for efficient workflow lookups.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "Document riverworkflow in CHANGELOG"
```

---

## Self-Review Notes

- **Spec coverage:** Section 3 (architecture) → Tasks 1, 3-9, 19. Section 4 (public API) → Tasks 11-18, 20, 21. Section 5 (data flow) → Tasks 7-9, 19, 20. Section 6 (validation) → Task 15. Section 7 (edge cases) → covered by driver conformance + scheduler tests. Section 8 (testing) → Tasks 5, 22, 23, 25-27. Section 9 (out-of-scope) → no tasks needed.
- **Placeholder scan:** A small number of "confirm against existing helper name" notes appear in Tasks 4, 7, 8, 9, 16, 18, 19, 20, 22, 23. These are real verify-against-codebase callouts, not placeholders — each one tells the engineer the specific grep/file to consult.
- **Type consistency:** `WorkflowTask.Name` (capitalized) used everywhere; `WorkflowOpts` fields (`IgnoreCancelledDeps`, `IgnoreDiscardedDeps`, `IgnoreDeletedDeps`) used consistently in Tasks 13, 16, 17. `MetadataKey*` constants from Task 2 reused verbatim in Tasks 5, 6, 8, 9, 16, 21. `JobUpdateWorkflowReady`/`JobGetWorkflowTasks`/`JobCancelWorkflow` names match across riverdriver interface, conformance tests, all three driver impls, and scheduler.
- **Risk:** Task 16's note about `river.InsertOpts.State` is a small unknown — if it doesn't exist, that task fans out by one commit. Track as the first thing to verify when executing Phase 4.
