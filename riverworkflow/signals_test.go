package riverworkflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/river/riverworkflow"
)

// buildSignalsWorkflow creates a Workflow handle bound to a fresh DB schema for
// signal API tests. It returns the workflow handle and the client.
func buildSignalsWorkflow(ctx context.Context, t *testing.T) (*riverworkflow.Workflow[pgx.Tx], *riverworkflow.Client[pgx.Tx]) {
	t.Helper()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	t.Cleanup(func() { dbPool.Close() })

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	// No workers needed — these tests only exercise the signal emit/list API
	// against the DB; they do not start the client or run any jobs.
	workers := river.NewWorkers()

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Schema:  schema,
			Workers: workers,
		},
	})
	require.NoError(t, err)

	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "signals-test"})
	return wf, client
}

// TestWorkflowSignals_EmitAndList verifies the round-trip: Emit a signal with
// a structured payload, then List it back and confirm the payload is preserved.
func TestWorkflowSignals_EmitAndList(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, _ := buildSignalsWorkflow(ctx, t)
	sigs := wf.Signals()

	payload := map[string]any{"ok": true}
	emitted, err := sigs.Emit(ctx, "approved", payload, nil)
	require.NoError(t, err)
	require.NotNil(t, emitted)
	require.Positive(t, emitted.ID)
	require.Equal(t, wf.ID(), emitted.WorkflowID)
	require.Equal(t, "approved", emitted.SignalKey)

	listed, err := sigs.List(ctx, nil)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, emitted.ID, listed[0].ID)
	require.JSONEq(t, `{"ok":true}`, string(listed[0].Payload))
}

// TestWorkflowSignals_IdempotentNoOp verifies that emitting twice with the same
// IdempotencyKey and identical payload returns the original signal row.
func TestWorkflowSignals_IdempotentNoOp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, _ := buildSignalsWorkflow(ctx, t)
	sigs := wf.Signals()

	payload := map[string]any{"ok": true}
	opts := &riverworkflow.WorkflowSignalEmitOpts{IdempotencyKey: "ikey-1"}

	first, err := sigs.Emit(ctx, "approved", payload, opts)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := sigs.Emit(ctx, "approved", payload, opts)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, first.ID, second.ID, "idempotent re-emit must return the same row ID")

	// Confirm only one row in the DB.
	all, err := sigs.List(ctx, nil)
	require.NoError(t, err)
	require.Len(t, all, 1)
}

// TestWorkflowSignals_PayloadMismatch verifies that emitting with the same
// IdempotencyKey but a different payload surfaces ErrSignalPayloadMismatch.
func TestWorkflowSignals_PayloadMismatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, _ := buildSignalsWorkflow(ctx, t)
	sigs := wf.Signals()

	opts := &riverworkflow.WorkflowSignalEmitOpts{IdempotencyKey: "ikey-conflict"}

	_, err := sigs.Emit(ctx, "approved", map[string]any{"ok": true}, opts)
	require.NoError(t, err)

	_, err = sigs.Emit(ctx, "approved", map[string]any{"ok": false}, opts)
	require.Error(t, err)
	require.True(t, errors.Is(err, riverworkflow.ErrSignalPayloadMismatch),
		"expected ErrSignalPayloadMismatch, got: %v", err)
	require.True(t, errors.Is(err, rivertype.ErrWorkflowSignalPayloadMismatch),
		"ErrSignalPayloadMismatch must wrap the rivertype sentinel")
}

// TestWorkflowSignals_LatestForTask verifies that LatestForTask returns the
// most recently created signal for a key when multiple signals exist.
func TestWorkflowSignals_LatestForTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, _ := buildSignalsWorkflow(ctx, t)
	sigs := wf.Signals()

	// Emit two signals with different payloads and no idempotency key.
	first, err := sigs.Emit(ctx, "approved", map[string]any{"n": 1}, nil)
	require.NoError(t, err)
	second, err := sigs.Emit(ctx, "approved", map[string]any{"n": 2}, nil)
	require.NoError(t, err)
	require.Greater(t, second.ID, first.ID)

	latest, err := sigs.LatestForTask(ctx, "some-task", "approved", nil)
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.Equal(t, second.ID, latest.ID, "LatestForTask must return the highest-ID signal")
}

// TestWorkflowSignals_ListForTask verifies that ListForTask filters by key.
func TestWorkflowSignals_ListForTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wf, _ := buildSignalsWorkflow(ctx, t)
	sigs := wf.Signals()

	_, err := sigs.Emit(ctx, "approved", map[string]any{"ok": true}, nil)
	require.NoError(t, err)
	_, err = sigs.Emit(ctx, "rejected", map[string]any{"ok": false}, nil)
	require.NoError(t, err)

	approved, err := sigs.ListForTask(ctx, "some-task", "approved", nil)
	require.NoError(t, err)
	require.Len(t, approved, 1)
	require.Equal(t, "approved", approved[0].SignalKey)

	all, err := sigs.List(ctx, nil)
	require.NoError(t, err)
	require.Len(t, all, 2)
}
