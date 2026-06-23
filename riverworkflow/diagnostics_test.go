package riverworkflow_test

import (
	"context"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/riverworkflow"
)

// diagTestArgs is a minimal job args type for diagnostics integration tests.
type diagTestArgs struct{}

func (diagTestArgs) Kind() string { return "riverworkflow_diag_test" }

// diagTestWorker is a no-op worker for diagTestArgs.
type diagTestWorker struct {
	river.WorkerDefaults[diagTestArgs]
}

func (w *diagTestWorker) Work(_ context.Context, _ *river.Job[diagTestArgs]) error {
	return nil
}

// buildDiagnosticsWorkflow creates a Client and a Workflow handle bound to a
// fresh DB schema for diagnostics tests, and inserts tasks into the DB.
// Returns the workflow handle, client, and the inserted workflow ID.
func buildDiagnosticsWorkflow(ctx context.Context, t *testing.T) (*riverworkflow.Workflow[pgx.Tx], *riverworkflow.Client[pgx.Tx]) {
	t.Helper()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	t.Cleanup(func() { dbPool.Close() })

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &diagTestWorker{})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Schema:  schema,
			Workers: workers,
		},
	})
	require.NoError(t, err)

	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "diagnostics-test"})
	return wf, client
}

// TestWaitDiagnostics_SignalGated tests that WaitDiagnostics correctly reports
// the wait phase before and after a signal is emitted.
func TestWaitDiagnostics_SignalGated(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, client := buildDiagnosticsWorkflow(ctx, t)

	// Build a workflow with:
	// - "prereq": a normal task with no wait (depends on nothing)
	// - "gate": a signal-gated task waiting for "approved" signal
	wf.Add("prereq", diagTestArgs{}, nil, nil)
	wf.Add("gate", diagTestArgs{}, nil, &riverworkflow.WorkflowTaskOpts{
		Wait: &riverworkflow.WaitSpec{
			Terms: []riverworkflow.WaitTermSpec{
				riverworkflow.WaitTermSignal("approved", "approved", "payload.ok").Label("Needs approval"),
			},
			Expr: "approved",
		},
	})

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	require.Len(t, prep.Jobs, 2)

	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	// BEFORE emitting the signal: phase should be pending, expr=false, term result=false
	diag, err := wf.WaitDiagnostics(ctx, "gate", nil)
	require.NoError(t, err)
	require.NotNil(t, diag)
	require.Equal(t, riverworkflow.WaitPhasePending, diag.Phase)
	require.False(t, diag.ExprResult)
	require.Len(t, diag.Terms, 1)
	require.Equal(t, "approved", diag.Terms[0].Name)
	require.False(t, diag.Terms[0].Result)

	// Emit the approval signal
	_, err = wf.Signals().Emit(ctx, "approved", map[string]any{"ok": true}, nil)
	require.NoError(t, err)

	// AFTER emitting: phase should be resolved, expr=true, term result=true
	diag2, err := wf.WaitDiagnostics(ctx, "gate", nil)
	require.NoError(t, err)
	require.NotNil(t, diag2)
	require.Equal(t, riverworkflow.WaitPhaseResolved, diag2.Phase)
	require.True(t, diag2.ExprResult)
	require.Len(t, diag2.Terms, 1)
	require.Equal(t, "approved", diag2.Terms[0].Name)
	require.True(t, diag2.Terms[0].Result)
	// Label should be carried through
	require.Equal(t, "Needs approval", diag2.Terms[0].Label)
}

// TestWaitDiagnostics_NoWait tests that a task without a wait spec returns WaitPhaseNoWait.
func TestWaitDiagnostics_NoWait(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, client := buildDiagnosticsWorkflow(ctx, t)

	wf.Add("simple", diagTestArgs{}, nil, nil)

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)

	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	diag, err := wf.WaitDiagnostics(ctx, "simple", nil)
	require.NoError(t, err)
	require.NotNil(t, diag)
	require.Equal(t, riverworkflow.WaitPhaseNoWait, diag.Phase)
}

// TestWaitDiagnostics_MissingTask tests that a missing task name returns an error.
func TestWaitDiagnostics_MissingTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, client := buildDiagnosticsWorkflow(ctx, t)

	wf.Add("exists", diagTestArgs{}, nil, nil)

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)

	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	_, err = wf.WaitDiagnostics(ctx, "does-not-exist", nil)
	require.Error(t, err)
}
