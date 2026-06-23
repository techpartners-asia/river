package riverworkflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverworkflow/internal/workflowid"
)

// WorkflowOpts configures a new workflow.
type WorkflowOpts struct {
	// DeadlineAt sets an absolute moment past which any still non-terminal
	// task in this workflow will be cancelled by the workflow scheduler with
	// reason "workflow deadline exceeded". The zero value means no deadline.
	//
	// Only tasks in pending, scheduled, available, and retryable states are
	// eligible for deadline cancellation; running tasks are left to finish
	// or be cancelled through the regular cancellation path.
	DeadlineAt time.Time

	// ID overrides the workflow's automatically generated identifier. Workflow
	// IDs must be globally unique; leave empty to use the auto-generated ULID.
	ID string

	// IgnoreCancelledDeps, when true, causes any cancelled dependency to be
	// treated as successful for the purpose of promoting dependents. Can be
	// overridden per task via [WorkflowTaskOpts].
	IgnoreCancelledDeps bool

	// IgnoreDeletedDeps mirrors IgnoreCancelledDeps for deleted dependencies.
	IgnoreDeletedDeps bool

	// IgnoreDiscardedDeps mirrors IgnoreCancelledDeps for discarded
	// dependencies.
	IgnoreDiscardedDeps bool

	// Name is an optional human-readable workflow label stored alongside each
	// task's metadata.
	Name string
}

// WorkflowTaskOpts configures a task being added to a workflow.
type WorkflowTaskOpts struct {
	// Deps lists the task names that must reach a terminal state before this
	// task becomes eligible to run.
	Deps []string

	// IgnoreCancelledDeps overrides the workflow-level setting when non-nil.
	IgnoreCancelledDeps *bool

	// IgnoreDeletedDeps overrides the workflow-level setting when non-nil.
	IgnoreDeletedDeps *bool

	// IgnoreDiscardedDeps overrides the workflow-level setting when non-nil.
	IgnoreDiscardedDeps *bool

	// Wait gates this task behind a CEL expression over signals, timers, and
	// dependency outputs. A wait-bearing task stays pending until the workflow
	// scheduler resolves the wait, independent of dependency completion.
	Wait *WaitSpec
}

// WorkflowTask is a handle to a task added to a workflow. The Name field can
// be used to reference this task in subsequent [Workflow.Add] calls' Deps.
type WorkflowTask struct {
	Name string

	args            river.JobArgs
	deps            []string
	ignoreCancelled *bool
	ignoreDeleted   *bool
	ignoreDiscarded *bool
	jobOpts         *river.InsertOpts
	wait            *WaitSpec
}

// Workflow is a builder for a directed acyclic graph of River jobs. Tasks are
// added via [Workflow.Add], then [Workflow.Prepare] is called to validate the
// DAG and render job-insertion parameters.
type Workflow[TTx any] struct {
	id     string
	name   string
	opts   WorkflowOpts
	tasks  []*WorkflowTask
	driver riverdriver.Driver[TTx]
	exec   riverdriver.Executor
	schema string
}

// ID returns the workflow's unique identifier.
func (w *Workflow[TTx]) ID() string { return w.id }

// Name returns the workflow's optional human-readable label.
func (w *Workflow[TTx]) Name() string { return w.name }

// Add appends a task to the workflow. taskName must be unique within the
// workflow. taskOpts may be nil for tasks with no dependencies.
func (w *Workflow[TTx]) Add(taskName string, args river.JobArgs, jobOpts *river.InsertOpts, taskOpts *WorkflowTaskOpts) *WorkflowTask {
	var (
		deps []string
		igC  *bool
		igDc *bool
		igDe *bool
		wait *WaitSpec
	)
	if taskOpts != nil {
		deps = append([]string(nil), taskOpts.Deps...)
		igC = taskOpts.IgnoreCancelledDeps
		igDc = taskOpts.IgnoreDiscardedDeps
		igDe = taskOpts.IgnoreDeletedDeps
		wait = taskOpts.Wait
	}

	task := &WorkflowTask{
		Name:            taskName,
		args:            args,
		deps:            deps,
		ignoreCancelled: igC,
		ignoreDeleted:   igDe,
		ignoreDiscarded: igDc,
		jobOpts:         jobOpts,
		wait:            wait,
	}
	w.tasks = append(w.tasks, task)
	return task
}

// WorkflowPrepareResult is returned by [Workflow.Prepare]. Jobs is a slice of
// [river.InsertManyParams] ready to be passed to [river.Client.InsertMany] or
// [river.Client.InsertManyTx].
type WorkflowPrepareResult struct {
	WorkflowID string
	Jobs       []river.InsertManyParams
}

// Prepare validates the workflow's DAG and renders job-insertion parameters.
// See [Workflow.PrepareTx] for the transactional variant.
func (w *Workflow[TTx]) Prepare(_ context.Context) (*WorkflowPrepareResult, error) {
	return w.prepare()
}

// PrepareTx validates the workflow's DAG and renders job-insertion parameters.
// It exists for API symmetry with riverpro and to leave room for future
// transaction-aware preparation. As of v1 it does not touch the database.
func (w *Workflow[TTx]) PrepareTx(_ context.Context, _ TTx) (*WorkflowPrepareResult, error) {
	return w.prepare()
}

func (w *Workflow[TTx]) prepare() (*WorkflowPrepareResult, error) {
	if err := w.validate(); err != nil {
		return nil, err
	}

	jobs := make([]river.InsertManyParams, 0, len(w.tasks))
	for _, t := range w.tasks {
		opts, err := w.renderTaskOpts(t)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, river.InsertManyParams{
			Args:       t.args,
			InsertOpts: opts,
		})
	}
	return &WorkflowPrepareResult{WorkflowID: w.id, Jobs: jobs}, nil
}

func (w *Workflow[TTx]) renderTaskOpts(t *WorkflowTask) (*river.InsertOpts, error) {
	var opts river.InsertOpts
	if t.jobOpts != nil {
		opts = *t.jobOpts
	}

	// Use map[string]json.RawMessage so existing numeric values (e.g. Snowflake
	// IDs, nanosecond timestamps) are kept as opaque JSON bytes and never
	// round-tripped through float64.
	metadata := map[string]json.RawMessage{}
	if len(opts.Metadata) > 0 {
		if err := json.Unmarshal(opts.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("riverworkflow: parse existing metadata for task %q: %w", t.Name, err)
		}
	}

	// inject encodes value and stores it, workflow keys override existing ones.
	inject := func(key string, value any) {
		enc, _ := json.Marshal(value)
		metadata[key] = enc
	}
	inject(rivercommon.MetadataKeyWorkflowID, w.id)
	if w.name != "" {
		inject(rivercommon.MetadataKeyWorkflowName, w.name)
	}
	inject(rivercommon.MetadataKeyWorkflowTask, t.Name)
	if !w.opts.DeadlineAt.IsZero() {
		// UTC + RFC3339 so the scheduler's wall-clock comparison stays
		// reader-agnostic across pod and DB locales.
		inject(rivercommon.MetadataKeyWorkflowDeadlineAt, w.opts.DeadlineAt.UTC().Format(time.RFC3339Nano))
	}
	if len(t.deps) > 0 {
		inject(rivercommon.MetadataKeyWorkflowDeps, t.deps)
		opts.Pending = true
	}
	if t.wait != nil {
		inject(rivercommon.MetadataKeyWorkflowWait, t.wait)
		opts.Pending = true
	}

	applyIgnore := func(taskFlag *bool, workflowFlag bool, key string) {
		switch {
		case taskFlag != nil:
			if *taskFlag {
				inject(key, true)
			}
		case workflowFlag:
			inject(key, true)
		}
	}
	applyIgnore(t.ignoreCancelled, w.opts.IgnoreCancelledDeps, rivercommon.MetadataKeyWorkflowIgnoreCancelledDeps)
	applyIgnore(t.ignoreDiscarded, w.opts.IgnoreDiscardedDeps, rivercommon.MetadataKeyWorkflowIgnoreDiscardedDeps)
	applyIgnore(t.ignoreDeleted, w.opts.IgnoreDeletedDeps, rivercommon.MetadataKeyWorkflowIgnoreDeletedDeps)

	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("riverworkflow: marshal metadata for task %q: %w", t.Name, err)
	}
	opts.Metadata = encoded
	return &opts, nil
}

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
		if t.wait != nil {
			if err := t.wait.Validate(); err != nil {
				return fmt.Errorf("task %q: %w", t.Name, err)
			}
		}
	}

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

// newWorkflow is the package-internal constructor; [Client.NewWorkflow] is the
// public entry point.
func newWorkflow[TTx any](opts *WorkflowOpts, driver riverdriver.Driver[TTx], schema string) *Workflow[TTx] {
	if opts == nil {
		opts = &WorkflowOpts{}
	}
	id := opts.ID
	if id == "" {
		id = workflowid.New()
	}
	w := &Workflow[TTx]{
		id:     id,
		name:   opts.Name,
		opts:   *opts,
		driver: driver,
		schema: schema,
	}
	if driver != nil {
		w.exec = driver.GetExecutor()
	}
	return w
}
