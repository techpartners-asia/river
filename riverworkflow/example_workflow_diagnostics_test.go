package riverworkflow_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/riverworkflow"
)

// DiagnosticsApprovalArgs is a sample River JobArgs for a signal-gated workflow task.
type DiagnosticsApprovalArgs struct {
	Label string `json:"label"`
}

func (DiagnosticsApprovalArgs) Kind() string { return "diagnostics_approval" }

// DiagnosticsApprovalWorker performs a no-op so the example focuses on the diagnostics mechanics.
type DiagnosticsApprovalWorker struct {
	river.WorkerDefaults[DiagnosticsApprovalArgs]
}

func (*DiagnosticsApprovalWorker) Work(_ context.Context, _ *river.Job[DiagnosticsApprovalArgs]) error {
	return nil
}

// Example_workflowWaitDiagnostics demonstrates a two-task workflow where the second
// task waits on a signal. The example calls WaitDiagnostics before and after emitting
// the signal, demonstrating that the diagnostics accurately reports the pending phase
// before the signal is received and the resolved phase after.
func Example_workflowWaitDiagnostics() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Set up the database schema using the test utility (required because the
	// example will call WaitDiagnostics and query the workflow state).
	schema := riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &DiagnosticsApprovalWorker{})

	// Create a workflow client.
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
	})
	if err != nil {
		panic(err)
	}

	// Build a workflow with two tasks: "task1" has no dependencies, and "task2"
	// depends on "task1" but is gated by a signal with key "approved". The
	// approval signal evaluates the CEL expression "approved".
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "wait-diagnostics demo"})
	wf.Add("task1", DiagnosticsApprovalArgs{Label: "prerequisite"}, nil, nil)
	wf.Add("task2", DiagnosticsApprovalArgs{Label: "approval required"}, nil, &riverworkflow.WorkflowTaskOpts{
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

	// Query diagnostics BEFORE emitting the signal. The phase should be pending.
	diag, err := wf.WaitDiagnostics(ctx, "task2", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(diag.Phase)

	// Emit the approval signal to unblock the task2 wait.
	_, err = wf.Signals().Emit(ctx, "approved", map[string]any{"ok": true}, nil)
	if err != nil {
		panic(err)
	}

	// Query diagnostics AFTER emitting the signal. The phase should be resolved.
	diag2, err := wf.WaitDiagnostics(ctx, "task2", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(diag2.Phase)

	// Output:
	// pending
	// resolved
}
