package riverworkflow

import "errors"

// ErrWorkflowDepCycle is returned by [Workflow.Prepare] when the workflow's
// task graph contains a cycle.
var ErrWorkflowDepCycle = errors.New("riverworkflow: dependency cycle detected")

// ErrWorkflowDepUnknown is returned by [Workflow.Prepare] when a task lists a
// dependency that doesn't correspond to any added task.
var ErrWorkflowDepUnknown = errors.New("riverworkflow: task references unknown dependency")

// ErrWorkflowEmpty is returned by [Workflow.Prepare] when the workflow has no
// tasks added.
var ErrWorkflowEmpty = errors.New("riverworkflow: workflow has no tasks")

// ErrWorkflowTaskNameDuplicate is returned by [Workflow.Prepare] when two
// tasks share the same name within the workflow.
var ErrWorkflowTaskNameDuplicate = errors.New("riverworkflow: duplicate task name")

// ErrWorkflowTaskNameEmpty is returned by [Workflow.Prepare] when a task was
// added with an empty name.
var ErrWorkflowTaskNameEmpty = errors.New("riverworkflow: task name is empty")

// ErrWorkflowTaskOutputMissing is returned by [WorkflowTasks.Output] when the
// named task has no recorded output yet.
var ErrWorkflowTaskOutputMissing = errors.New("riverworkflow: task has no recorded output")
