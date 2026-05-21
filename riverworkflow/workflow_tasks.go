package riverworkflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// LoadDepsOpts controls dependency loading via [Workflow.LoadDeps].
type LoadDepsOpts struct {
	// Recursive walks the dependency tree transitively when true.
	Recursive bool
}

// WorkflowTasks is a name-keyed collection of workflow task rows returned by
// [Workflow.LoadAll], [Workflow.LoadDeps], and their Tx variants.
type WorkflowTasks struct {
	byName map[string]*rivertype.JobRow
}

// Get returns the task with the given name. An error is returned when the
// task is not present in the collection.
func (wt *WorkflowTasks) Get(taskName string) (*rivertype.JobRow, error) {
	row, ok := wt.byName[taskName]
	if !ok {
		return nil, fmt.Errorf("riverworkflow: task %q not found", taskName)
	}
	return row, nil
}

// Output decodes the task's recorded output into out. Returns
// [ErrWorkflowTaskOutputMissing] when no output was recorded by the worker.
func (wt *WorkflowTasks) Output(taskName string, out any) error {
	row, err := wt.Get(taskName)
	if err != nil {
		return err
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(row.Metadata, &meta); err != nil {
		return fmt.Errorf("riverworkflow: parse metadata for task %q: %w", taskName, err)
	}
	raw, ok := meta[rivertype.MetadataKeyOutput]
	if !ok {
		return ErrWorkflowTaskOutputMissing
	}
	return json.Unmarshal(raw, out)
}

// LoadAll reads every task belonging to the workflow.
func (w *Workflow[TTx]) LoadAll(ctx context.Context) (*WorkflowTasks, error) {
	return w.loadAllOnExec(ctx, w.exec)
}

// LoadAllTx is the transactional variant of [Workflow.LoadAll].
func (w *Workflow[TTx]) LoadAllTx(ctx context.Context, tx TTx) (*WorkflowTasks, error) {
	return w.loadAllOnExec(ctx, w.driver.UnwrapExecutor(tx))
}

// LoadDeps reads the direct (or, with [LoadDepsOpts.Recursive], transitive)
// dependencies of taskName from the database. opts may be nil for direct deps.
func (w *Workflow[TTx]) LoadDeps(ctx context.Context, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error) {
	return w.loadDepsOnExec(ctx, w.exec, taskName, opts)
}

// LoadDepsTx is the transactional variant of [Workflow.LoadDeps].
func (w *Workflow[TTx]) LoadDepsTx(ctx context.Context, tx TTx, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error) {
	return w.loadDepsOnExec(ctx, w.driver.UnwrapExecutor(tx), taskName, opts)
}

func (w *Workflow[TTx]) loadAllOnExec(ctx context.Context, exec riverdriver.Executor) (*WorkflowTasks, error) {
	if exec == nil {
		return nil, fmt.Errorf("riverworkflow: workflow has no driver bound")
	}
	rows, err := exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
		Schema:     w.schema,
		WorkflowID: w.id,
	})
	if err != nil {
		return nil, err
	}
	return tasksFromRows(rows), nil
}

func (w *Workflow[TTx]) loadDepsOnExec(ctx context.Context, exec riverdriver.Executor, taskName string, opts *LoadDepsOpts) (*WorkflowTasks, error) {
	all, err := w.loadAllOnExec(ctx, exec)
	if err != nil {
		return nil, err
	}
	recursive := opts != nil && opts.Recursive
	return walkDeps(all, taskName, recursive), nil
}

func tasksFromRows(rows []*rivertype.JobRow) *WorkflowTasks {
	out := &WorkflowTasks{byName: make(map[string]*rivertype.JobRow, len(rows))}
	for _, r := range rows {
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(r.Metadata, &meta); err != nil {
			slog.Default().Warn("riverworkflow: skipping task with unparseable metadata",
				slog.Int64("job_id", r.ID),
				slog.String("error", err.Error()))
			continue
		}
		raw, ok := meta[rivercommon.MetadataKeyWorkflowTask]
		if !ok {
			// Not a workflow task; skip silently — that's normal.
			continue
		}
		var name string
		if err := json.Unmarshal(raw, &name); err != nil || name == "" {
			slog.Default().Warn("riverworkflow: skipping task with malformed task name",
				slog.Int64("job_id", r.ID),
				slog.String("error", fmt.Sprintf("%v", err)))
			continue
		}
		out.byName[name] = r
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
		if err := json.Unmarshal(row.Metadata, &meta); err != nil {
			return
		}
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
