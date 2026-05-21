package riverworkflow_test

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/riverworkflow"
)

// SortArgs is a sample River JobArgs implementation.
type SortArgs struct {
	Strings []string `json:"strings"`
}

func (SortArgs) Kind() string { return "sort" }

// SortWorker performs a no-op so the example focuses on the workflow shape.
type SortWorker struct {
	river.WorkerDefaults[SortArgs]
}

func (*SortWorker) Work(_ context.Context, _ *river.Job[SortArgs]) error { return nil }

// Example_workflowFanOutFanIn demonstrates a workflow with one root task "a",
// two fan-out children "b1" and "b2", and one fan-in tail task "c".
func Example_workflowFanOutFanIn() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &SortWorker{})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
			Workers: workers,
		},
	})
	if err != nil {
		panic(err)
	}

	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "fan-out demo"})
	taskA := wf.Add("a", SortArgs{}, nil, nil)
	taskB1 := wf.Add("b1", SortArgs{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{taskA.Name}})
	taskB2 := wf.Add("b2", SortArgs{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{taskA.Name}})
	wf.Add("c", SortArgs{}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{taskB1.Name, taskB2.Name}})

	prep, err := wf.Prepare(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Printf("workflow %q has %d tasks\n", wf.Name(), len(prep.Jobs))
	// Output: workflow "fan-out demo" has 4 tasks
}
