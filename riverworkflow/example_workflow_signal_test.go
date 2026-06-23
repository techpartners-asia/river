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

// SignalApprovalArgs is a sample River JobArgs for a signal-gated workflow task.
type SignalApprovalArgs struct {
	Label string `json:"label"`
}

func (SignalApprovalArgs) Kind() string { return "signal_approval" }

// SignalApprovalWorker performs a no-op so the example focuses on the signal mechanics.
type SignalApprovalWorker struct {
	river.WorkerDefaults[SignalApprovalArgs]
}

func (*SignalApprovalWorker) Work(_ context.Context, _ *river.Job[SignalApprovalArgs]) error {
	return nil
}

// Example_workflowSignalApproval demonstrates a two-task workflow where the second
// task waits on a signal for human approval. The example emits the approval signal
// and the workflow completes, demonstrating end-to-end signal-gated workflow execution.
func Example_workflowSignalApproval() {
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
	river.AddWorker(workers, &SignalApprovalWorker{})

	// Create a workflow client with a fast scheduler so the example completes quickly.
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
			Interval: 100 * time.Millisecond,
		},
	})
	if err != nil {
		panic(err)
	}

	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	// Build a workflow with two tasks: "request" has no dependencies, and "approval"
	// depends on "request" but is gated by a signal with key "approved". The
	// approval signal evaluates the CEL expression "payload.ok".
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "signal-approval demo"})
	request := wf.Add("request", SignalApprovalArgs{Label: "submit request"}, nil, nil)
	wf.Add("approval", SignalApprovalArgs{Label: "human approval"}, nil, &riverworkflow.WorkflowTaskOpts{
		Deps: []string{request.Name},
		Wait: &riverworkflow.WaitSpec{
			Terms: []riverworkflow.WaitTermSpec{
				riverworkflow.WaitTermSignal("approved", "approved", "payload.ok"),
			},
			Expr: "approved",
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

	// Wait for the request task to complete.
	timeout := time.After(10 * time.Second)
	select {
	case <-subscribeChan:
		// request task completed
	case <-timeout:
		panic("timed out waiting for request task to complete")
	}

	// Emit the approval signal to unblock the approval task.
	_, err = wf.Signals().Emit(ctx, "approved", map[string]any{"ok": true}, nil)
	if err != nil {
		panic(err)
	}

	// Wait for the approval task to complete.
	timeout2 := time.After(10 * time.Second)
	select {
	case <-subscribeChan:
		// approval task completed
	case <-timeout2:
		panic("timed out waiting for approval task to complete after signal was emitted")
	}

	fmt.Println("approved")
	// Output: approved
}
