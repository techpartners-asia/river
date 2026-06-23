package riverworkflow_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/riverworkflow"
)

// TimerWaitArgs is a sample River JobArgs for a timer-gated workflow task.
type TimerWaitArgs struct {
	Label string `json:"label"`
}

func (TimerWaitArgs) Kind() string { return "timer_wait" }

// TimerWaitWorker performs a no-op so the example focuses on the wait mechanics.
type TimerWaitWorker struct {
	river.WorkerDefaults[TimerWaitArgs]
}

func (*TimerWaitWorker) Work(_ context.Context, _ *river.Job[TimerWaitArgs]) error {
	return nil
}

// Example_workflowWaitTimer demonstrates a two-task workflow where the second
// task waits on a timer for a short duration after the first task completes.
// The scheduler resolves the timer and promotes the second task, demonstrating
// end-to-end timer-gated workflow execution.
func Example_workflowWaitTimer() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Set up the database schema using the test utility (required because the
	// example will call Start() and actually run the workflow).
	schema := riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &TimerWaitWorker{})

	// Create a workflow client with a fast scheduler and timer-poller so the
	// example completes quickly.
	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level:       slog.LevelWarn,
				ReplaceAttr: slogutil.NoLevelTime,
			})),
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
			Schema:  schema,
			Workers: workers,
		},
		WorkflowScheduler: riverworkflow.WorkflowSchedulerConfig{
			Interval:                    100 * time.Millisecond,
			WorkflowTimerPollerInterval: 50 * time.Millisecond,
		},
	})
	if err != nil {
		panic(err)
	}

	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	// Build a workflow with two tasks: "step1" has no dependencies, and "step2"
	// depends on "step1" but is gated by a timer that fires 100ms after the
	// wait starts (i.e., after step1 completes and the wait is recorded).
	timerDur := 100 * time.Millisecond
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "timer-wait demo"})
	step1 := wf.Add("step1", TimerWaitArgs{Label: "first"}, nil, nil)
	wf.Add("step2", TimerWaitArgs{Label: "second"}, nil, &riverworkflow.WorkflowTaskOpts{
		Deps: []string{step1.Name},
		Wait: &riverworkflow.WaitSpec{
			Terms: []riverworkflow.WaitTermSpec{
				riverworkflow.WaitTermTimer(riverworkflow.TimerAfterWaitStarted("delay", timerDur)),
			},
			Expr: "delay",
		},
	})

	prep, err := wf.Prepare(ctx)
	if err != nil {
		panic(err)
	}

	_, err = client.InsertMany(ctx, prep.Jobs)
	if err != nil {
		panic(err)
	}

	if err := client.Start(ctx); err != nil {
		panic(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = client.Stop(stopCtx)
	}()

	// Wait for both tasks to complete.
	timeout := time.After(10 * time.Second)
	for completed := 0; completed < 2; {
		select {
		case <-subscribeChan:
			completed++
		case <-timeout:
			panic("timed out waiting for workflow completion")
		}
	}

	fmt.Println("workflow complete")
	// Output: workflow complete
}
