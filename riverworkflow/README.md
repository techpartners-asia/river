# riverworkflow

Workflow DAGs for [River](https://github.com/riverqueue/river).

`riverworkflow.Client` wraps `*river.Client` and adds methods to build, insert,
inspect, and cancel multi-step workflows whose tasks have declared dependencies
on each other. Tasks with unsatisfied dependencies sit in the `pending` state
and are promoted to `available` once their dependencies complete; failed
dependencies cascade cancellation (configurable per task or per workflow).

## Quickstart

```go
import (
    "github.com/riverqueue/river"
    "github.com/riverqueue/river/riverdriver/riverpgxv5"
    "github.com/riverqueue/river/riverworkflow"
)

client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
    Config: river.Config{
        Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 100}},
        Workers: workers,
    },
})
if err != nil {
    panic(err)
}

wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "billing"})
a := wf.Add("a", ArgsA{}, nil, nil)
b1 := wf.Add("b1", ArgsB{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{a.Name}})
b2 := wf.Add("b2", ArgsB{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{a.Name}})
wf.Add("c", ArgsC{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{b1.Name, b2.Name}})

prep, err := wf.Prepare(ctx)
if err != nil {
    panic(err)
}
if _, err := client.InsertMany(ctx, prep.Jobs); err != nil {
    panic(err)
}
```

See `example_workflow_test.go` for a runnable variant.

## Migration

Apply the `007_workflow_index` migration via the standard `rivermigrate`
tooling before deploying workflows. The migration adds an index on
`metadata->>'river:workflow_id'` (or its SQLite equivalent) for efficient
workflow lookups by the scheduler.

## Failure cascade

By default, when one of a task's dependencies ends up cancelled, discarded, or
deleted, the dependent task is cancelled instead of promoted. Three flags on
`WorkflowOpts` and `WorkflowTaskOpts` opt into treating a particular failure
mode as a successful completion for promotion purposes:

- `IgnoreCancelledDeps`
- `IgnoreDiscardedDeps`
- `IgnoreDeletedDeps`

Per-task flags (when non-nil) override the workflow-level defaults.

## Reading dependency state from a worker

Inside a worker, call `wf.LoadDeps(ctx, taskName, &riverworkflow.LoadDepsOpts{Recursive: true})`
to read sibling rows that you depend on, then `tasks.Output(name, &out)` to
decode JSON output recorded by an upstream worker via
`river.RecordOutput`.

## Waits: signals, timers, and CEL conditions

Beyond dependency DAGs, a task can carry a `Wait` that holds it until a
**signal** arrives, a **timer** fires, or a **CEL** condition over
signals/timers/dependency-outputs resolves true — with read-only
`WaitDiagnostics` for introspection. See
[`docs/workflow_wait.md`](../docs/workflow_wait.md) for the full guide.

## Driver support

All three OSS drivers are supported: `riverpgxv5`, `riverdatabasesql`, and
`riversqlite`.
