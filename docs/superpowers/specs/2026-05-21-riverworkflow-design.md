# Riverworkflow — DAG Workflows for River OSS

**Status:** Design approved 2026-05-21
**Authors:** @Munkherdene (with Claude)
**Scope:** New `riverworkflow` submodule that provides fan-out / fan-in workflow DAGs on top of the OSS River core. API mirrors the riverpro `Workflow` surface so users migrating off Pro can drop in.

## 1. Motivation

River OSS jobs run independently. Many real systems require multi-step pipelines where step B starts only after step A completes (data ETL, payment + receipt + email, multi-stage codec encode, etc.). Today River users either build this themselves on top of `JobCompleteTx` (manual, error-prone) or pay for riverpro.

This design adds a first-class workflow API that:

- Lets users declare a DAG of tasks with named dependencies.
- Inserts all tasks in one shot; tasks with unsatisfied deps sit in River's existing `pending` state until promoted.
- Promotes pending tasks to `available` when their deps complete.
- Cascades cancellation when a dep fails (configurable).
- Exposes the riverpro-shaped API verbatim so migration is mechanical.

## 2. Non-Goals (v1)

- No new state in the `river_job` table — workflows live entirely in `metadata`.
- No notifier / LISTEN integration. Promotion is purely polled by a leader-elected service.
- No automatic rescue of stuck pending workflow rows (operators use `WorkflowCancel`).
- No UI work in this repo — riverui consumes the same metadata.

## 3. Architecture

### 3.1 New module

```
riverworkflow/
  go.mod
  doc.go
  client.go               # Client[TTx] wraps *river.Client[TTx]; NewClient, NewWorkflow, WorkflowCancel(Tx), WorkflowFromExisting
  workflow.go             # Workflow, WorkflowOpts, WorkflowTaskOpts, Add, Prepare(Tx), DAG validation
  workflow_tasks.go       # WorkflowTasks, Get, Output; LoadDeps(Tx), LoadAll(Tx)
  errors.go               # sentinel errors
  workflow_test.go
  workflow_tasks_test.go
  internal/
    workflowscheduler/
      workflow_scheduler.go
      workflow_scheduler_test.go
  riverworkflowmigrate/   # (optional v1) — migrations live in main rivermigrate line instead
```

`riverworkflow` is its own Go module (own `go.mod`/`go.sum`), like `rivermigrate`, `rivertest`, and `riverlog`. Added to top-level `go.work`.

### 3.2 Driver additions

`riverdriver` gains three new methods, mirrored across `riverpgxv5`, `riverdatabasesql`, and `riversqlite`. Treat `riverdriver` as the internal adapter seam per `AGENTS.md` — breaking changes allowed.

```go
type Driver[TTx any] interface {
    // ... existing ...

    JobGetWorkflowTasks(ctx context.Context, params *JobGetWorkflowTasksParams) ([]*rivertype.JobRow, error)
    JobUpdateWorkflowReady(ctx context.Context, params *JobUpdateWorkflowReadyParams) ([]*rivertype.JobRow, error)
    JobCancelWorkflow(ctx context.Context, params *JobCancelWorkflowParams) ([]*rivertype.JobRow, error)
}

type JobGetWorkflowTasksParams struct {
    Schema     string
    WorkflowID string
    TaskNames  []string // optional filter; nil/empty → all tasks
}

type JobUpdateWorkflowReadyParams struct {
    Schema string
    Max    int  // batch size, e.g. 1000
    Now    time.Time
}

type JobCancelWorkflowParams struct {
    Schema     string
    WorkflowID string
    Now        time.Time
    Reason     string // written to AttemptError / metadata
}
```

`riverdriver/riverdrivertest` adds a conformance suite covering all three.

### 3.3 Metadata keys (added to `internal/rivercommon`)

```go
const (
    MetadataKeyWorkflowID                  = "river:workflow_id"           // ULID string
    MetadataKeyWorkflowName                = "river:workflow_name"         // optional label
    MetadataKeyWorkflowTask                = "river:workflow_task"         // unique-within-workflow name
    MetadataKeyWorkflowDeps                = "river:workflow_deps"         // JSON array of task names
    MetadataKeyWorkflowIgnoreCancelledDeps = "river:workflow_ignore_cancelled_deps" // bool, only set when true
    MetadataKeyWorkflowIgnoreDiscardedDeps = "river:workflow_ignore_discarded_deps"
    MetadataKeyWorkflowIgnoreDeletedDeps   = "river:workflow_ignore_deleted_deps"
)
```

Prefix convention follows the existing `river:periodic_job_id` / `river:resumable_step` keys.

### 3.4 Maintenance service

`internal/workflowscheduler.WorkflowScheduler` is a `startstop.Service` registered into `QueueMaintainer` alongside `JobScheduler` and `JobRescuer`. Leader-elected (single instance per schema).

Loop:

```text
every Interval (default 5s):
    rows, err := driver.JobUpdateWorkflowReady(ctx, {Max: 1000, Now: time.Now(), Schema: ...})
    if rows are full batch → loop again immediately (drain)
    emit TestSignal so tests can wait deterministically
```

### 3.5 Migration

`rivermigrate` line `main`: one new versioned migration (next sequential number) creating an index that the scheduler query relies on.

```sql
-- Postgres
CREATE INDEX river_job_workflow_id_idx
  ON river_job ((metadata->>'river:workflow_id'), state)
  WHERE metadata ? 'river:workflow_id';

-- SQLite (analogous, generated column / expression depending on what River's SQLite migrations already use)
CREATE INDEX river_job_workflow_id_idx
  ON river_job (json_extract(metadata, '$."river:workflow_id"'), state)
  WHERE json_extract(metadata, '$."river:workflow_id"') IS NOT NULL;
```

Rollback drops the index.

## 4. Public API

```go
package riverworkflow

// ---------- Client ----------

type Client[TTx any] struct {
    *river.Client[TTx] // embedded so all river methods pass through unchanged
    // unexported fields
}

func NewClient[TTx any](driver riverdriver.Driver[TTx], config *Config) (*Client[TTx], error)

type Config struct {
    river.Config
    WorkflowScheduler WorkflowSchedulerConfig
}

type WorkflowSchedulerConfig struct {
    Interval  time.Duration // default 5s
    BatchSize int           // default 1000
}

// ---------- Workflow construction ----------

type WorkflowOpts struct {
    ID                  string
    Name                string
    IgnoreCancelledDeps bool
    IgnoreDiscardedDeps bool
    IgnoreDeletedDeps   bool
}

type WorkflowTaskOpts struct {
    Deps                []string
    IgnoreCancelledDeps *bool // override workflow default when non-nil
    IgnoreDiscardedDeps *bool
    IgnoreDeletedDeps   *bool
}

type Workflow[TTx any] struct{ /* ... */ }

func (c *Client[TTx]) NewWorkflow(opts *WorkflowOpts) *Workflow[TTx]
func (c *Client[TTx]) WorkflowFromExisting(jobRow *rivertype.JobRow, opts *WorkflowOpts) (*Workflow[TTx], error)

type WorkflowTask struct {
    Name string
    // unexported: args, jobOpts, deps, ignoreFlags
}

func (w *Workflow[TTx]) Add(taskName string, args river.JobArgs, jobOpts *river.InsertOpts, taskOpts *WorkflowTaskOpts) *WorkflowTask

type WorkflowPrepareResult struct {
    WorkflowID string
    Jobs       []river.InsertManyParams
}

func (w *Workflow[TTx]) Prepare(ctx context.Context) (*WorkflowPrepareResult, error)
func (w *Workflow[TTx]) PrepareTx(ctx context.Context, tx TTx) (*WorkflowPrepareResult, error)

// ---------- Cancel ----------

type WorkflowCancelResult struct {
    CancelledJobs []*rivertype.JobRow
}

func (c *Client[TTx]) WorkflowCancel(ctx context.Context, workflowID string) (*WorkflowCancelResult, error)
func (c *Client[TTx]) WorkflowCancelTx(ctx context.Context, tx TTx, workflowID string) (*WorkflowCancelResult, error)

// ---------- Inspect from inside a worker ----------

type WorkflowTasks struct{ /* keyed by task name */ }

func (wt *WorkflowTasks) Get(taskName string) (*rivertype.JobRow, error)
func (wt *WorkflowTasks) Output(taskName string, out any) error

type LoadDepsOpts struct {
    Recursive bool
}

func (w *Workflow[TTx]) LoadDeps(ctx context.Context, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error)
func (w *Workflow[TTx]) LoadDepsTx(ctx context.Context, tx TTx, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error)
func (w *Workflow[TTx]) LoadAll(ctx context.Context) (*WorkflowTasks, error)
func (w *Workflow[TTx]) LoadAllTx(ctx context.Context, tx TTx) (*WorkflowTasks, error)
```

### 4.1 Sentinel errors (`errors.go`)

```go
var (
    ErrWorkflowEmpty             = errors.New("workflow has no tasks")
    ErrWorkflowTaskNameEmpty     = errors.New("workflow task name is empty")
    ErrWorkflowTaskNameDuplicate = errors.New("workflow task name duplicated")
    ErrWorkflowDepUnknown        = errors.New("workflow task references unknown dep")
    ErrWorkflowDepCycle          = errors.New("workflow contains a dependency cycle")
    ErrWorkflowTaskOutputMissing = errors.New("workflow task has no recorded output")
)
```

## 5. Data Flow

### 5.1 Insert path

1. `client.NewWorkflow(opts)` → returns a fresh `Workflow` with `id = opts.ID || ulid.New()`.
2. `workflow.Add(name, args, jobOpts, taskOpts)` × N → in-memory builder.
3. `workflow.Prepare(ctx)`:
   1. Validate (Section 6).
   2. Render each task to `river.InsertManyParams`:
      - Inject the workflow metadata keys from §3.3 into `InsertOpts.Metadata`.
      - State assignment:
        - `len(taskOpts.Deps) == 0` → River's default (Available or Scheduled).
        - `len(taskOpts.Deps) > 0` → `state = pending`. `ScheduledAt` preserved on the row for the scheduler to honor.
   3. Return `WorkflowPrepareResult{WorkflowID, Jobs}`.
4. Caller invokes `client.InsertMany(ctx, result.Jobs)` (or `InsertManyTx`).

### 5.2 Promotion path

WorkflowScheduler tick → driver `JobUpdateWorkflowReady`.

The single SQL statement per dialect (Postgres shown; database/sql identical, SQLite uses `json_extract`):

```sql
WITH candidates AS (
  SELECT id, metadata, scheduled_at
  FROM river_job
  WHERE state = 'pending'
    AND metadata ? 'river:workflow_id'
  ORDER BY id
  FOR UPDATE SKIP LOCKED
  LIMIT @max
),
dep_states AS (
  SELECT
    c.id AS candidate_id,
    sib.state AS dep_state,
    sib.metadata->>'river:workflow_task' AS dep_task
  FROM candidates c
  JOIN river_job sib
    ON sib.metadata->>'river:workflow_id' = c.metadata->>'river:workflow_id'
   AND sib.metadata->>'river:workflow_task' = ANY(
         SELECT jsonb_array_elements_text(c.metadata->'river:workflow_deps')
       )
),
resolved AS (
  SELECT
    c.id,
    c.scheduled_at,
    bool_and(d.dep_state = 'completed') AS all_done,
    bool_or(
      d.dep_state IN ('cancelled','discarded')
      AND NOT COALESCE((c.metadata->>'river:workflow_ignore_'||d.dep_state||'_deps')::bool, false)
    ) AS any_failed,
    -- "deleted" handled separately because the sib row is missing
    count(*) AS dep_rows_found,
    (SELECT count(*) FROM jsonb_array_elements_text(c.metadata->'river:workflow_deps')) AS dep_rows_declared
  FROM candidates c
  LEFT JOIN dep_states d ON d.candidate_id = c.id
  GROUP BY c.id, c.scheduled_at, c.metadata
)
UPDATE river_job j
SET state = CASE
              WHEN any_failed THEN 'cancelled'
              WHEN dep_rows_found < dep_rows_declared
                   AND NOT COALESCE((j.metadata->>'river:workflow_ignore_deleted_deps')::bool, false)
                   THEN 'cancelled'
              WHEN all_done AND j.scheduled_at > @now THEN 'scheduled'
              WHEN all_done THEN 'available'
              ELSE 'pending'
            END,
    finalized_at = CASE WHEN any_failed OR dep_rows_found < dep_rows_declared THEN @now ELSE NULL END
FROM resolved r
WHERE j.id = r.id
  AND (r.all_done OR r.any_failed OR r.dep_rows_found < r.dep_rows_declared)
RETURNING j.*;
```

The driver returns the transitioned rows. The scheduler logs the counts and emits a `TestSignal`.

### 5.3 Cancel path

`WorkflowCancel(ctx, id)` → driver `JobCancelWorkflow` sets `state='cancelled', finalized_at=now()` for every non-finalized row with the given workflow ID, in a single `UPDATE ... RETURNING`.

### 5.4 LoadDeps / LoadAll

Backed by `JobGetWorkflowTasks`. Recursive walks are done in Go after pulling the whole workflow.

## 6. Validation (`Prepare`)

Run in this order, fail fast on first error:

1. `len(tasks) == 0` → `ErrWorkflowEmpty`.
2. For each task: `name == ""` → `ErrWorkflowTaskNameEmpty`.
3. Build a name→task map; second occurrence → `ErrWorkflowTaskNameDuplicate`.
4. For each task, each `dep`: dep not in map → `ErrWorkflowDepUnknown`.
5. Topological sort (Kahn's). Remaining nodes → `ErrWorkflowDepCycle` (error message lists the cycle).

## 7. Edge Cases & Failure Modes

| Case | Behavior |
|---|---|
| Snooze during workflow task | Task → Scheduled. Dependents remain Pending. On next success → Completed. |
| Retryable / running dep | Dependents stay Pending until terminal state. |
| Dep `cancelled`, `IgnoreCancelledDeps=true` | Treated as success for promotion purposes. |
| Dep `discarded`, `IgnoreDiscardedDeps=true` | Treated as success. |
| Dep row missing entirely (deleted out-of-band), `IgnoreDeletedDeps=false` | Dependent cancelled by scheduler. |
| Multi-schema | Each schema has its own scheduler tick + indexes; queries are schema-scoped. |
| Partial `InsertMany` failure | Caller is responsible (use `InsertManyTx`). Same failure surface River already exposes. |
| Concurrent manual cancel vs promotion | `FOR UPDATE SKIP LOCKED` plus the `state='pending'` guard prevents double transitions. |

## 8. Testing

### 8.1 `riverworkflow/`

- `TestWorkflow_Add` — invalid names, basic build.
- `TestWorkflow_Prepare` — exhaustive validation matrix; asserts state, metadata, and dep arrays per task.
- `TestWorkflow_PrepareTx` — uses `riverdbtest` for a real DB.
- `TestClient_WorkflowCancel` — non-finalized cancelled; finalized left alone.
- `TestClient_WorkflowFromExisting` — extends a live workflow.
- `TestWorkflow_LoadDeps` — recursive + non-recursive; partial state.
- `TestWorkflowTasks_Output` — happy path + `ErrWorkflowTaskOutputMissing`.

All tests follow `AGENTS.md`: `t.Parallel()` first, local `testBundle`, `setup(t)` helper, `require` not `assert`.

### 8.2 `riverdriver/riverdrivertest`

- `TestJobGetWorkflowTasks` — filter by name, return ordering.
- `TestJobUpdateWorkflowReady` — fan-out, fan-in, diamond, scheduled-at-in-future, each ignore flag, missing dep row.
- `TestJobCancelWorkflow` — bulk cancel + return value.

All three drivers run the same suite (per `riverdrivertest` conformance pattern).

### 8.3 `internal/workflowscheduler/`

- `TestWorkflowScheduler` — uses River's maintenance test harness + `testsignal.TestSignal` on each tick; asserts promotion happens within one tick after deps complete.

### 8.4 Examples (repo root, runnable via `go test ./...`)

- `example_workflow_test.go` — fan-out / fan-in (the user's sample).
- `example_workflow_cancel_test.go`.
- `example_workflow_load_deps_test.go`.

### 8.5 Lint / CI

- `make test` + `make test/race` + `make lint` all clean.
- New module wired into top-level `go.work`.
- Top-level Makefile already iterates submodules — verify `riverworkflow` picked up.

## 9. Out of Scope / Future Work

- LISTEN/NOTIFY trigger for sub-second promotion (we chose polling for v1).
- Workflow templates / reusable workflow definitions.
- Per-workflow retention policy beyond River's existing `JobCleaner`.
- River UI integration (separate repo).
- Workflow priorities (use job priorities per-task).

## 10. Open Questions

None at design time. Tracked in the implementation plan as they arise.
