package riverdrivertest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseWorkflowSignal[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()
		exec, driver := executorWithTx(ctx, t)
		return exec, &testBundle{driver: driver}
	}

	t.Run("WorkflowSignal", func(t *testing.T) {
		t.Parallel()

		t.Run("Emit_InsertsAndReturns", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Truncate(time.Second)
			sig, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID: "wf-001",
				SignalKey:  "order.ready",
				Payload:    []byte(`{"item":"widget"}`),
				Now:        now,
			})
			require.NoError(t, err)
			require.NotNil(t, sig)
			require.Positive(t, sig.ID)
			require.Equal(t, "wf-001", sig.WorkflowID)
			require.Equal(t, "order.ready", sig.SignalKey)
			require.WithinDuration(t, now, sig.CreatedAt, bundle.driver.TimePrecision())
			require.Nil(t, sig.IdempotencyKey)
			require.Nil(t, sig.ResolvedAt)
		})

		t.Run("Emit_IdempotentNoOp", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			ikey := ptrutil.Ptr("ikey-abc")
			payload := []byte(`{"item":"widget"}`)

			sig1, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID:     "wf-002",
				SignalKey:      "order.ready",
				Payload:        payload,
				IdempotencyKey: ikey,
				Now:            time.Now().UTC(),
			})
			require.NoError(t, err)
			require.NotNil(t, sig1)

			// Second emit with same idempotency_key + same payload → same row ID
			sig2, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID:     "wf-002",
				SignalKey:      "order.ready",
				Payload:        payload,
				IdempotencyKey: ikey,
				Now:            time.Now().UTC(),
			})
			require.NoError(t, err)
			require.NotNil(t, sig2)
			require.Equal(t, sig1.ID, sig2.ID, "idempotent re-emit must return the same row ID")

			// Only one row in DB
			sigs, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID: "wf-002",
				Max:        100,
			})
			require.NoError(t, err)
			require.Len(t, sigs, 1)
		})

		t.Run("Emit_PayloadMismatch", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			ikey := ptrutil.Ptr("ikey-conflict")

			_, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID:     "wf-003",
				SignalKey:      "order.ready",
				Payload:        []byte(`{"item":"widget"}`),
				IdempotencyKey: ikey,
				Now:            time.Now().UTC(),
			})
			require.NoError(t, err)

			// Same idempotency key, DIFFERENT payload → error
			_, err = exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID:     "wf-003",
				SignalKey:      "order.ready",
				Payload:        []byte(`{"item":"gadget"}`),
				IdempotencyKey: ikey,
				Now:            time.Now().UTC(),
			})
			require.Error(t, err)
			require.True(t, errors.Is(err, rivertype.ErrWorkflowSignalPayloadMismatch),
				"expected ErrWorkflowSignalPayloadMismatch, got: %v", err)
		})

		t.Run("Emit_NoIdempotencyKeyAlwaysInserts", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			// No idempotency key → two separate rows
			for i := 0; i < 2; i++ {
				_, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
					WorkflowID: "wf-004",
					SignalKey:  "order.ready",
					Payload:    []byte(`{}`),
					Now:        time.Now().UTC(),
				})
				require.NoError(t, err)
			}

			sigs, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID: "wf-004",
				Max:        100,
			})
			require.NoError(t, err)
			require.Len(t, sigs, 2, "two emits with nil idempotency_key must produce two rows")
		})

		t.Run("List_FiltersAndOrders", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			base := time.Now().UTC().Truncate(time.Second)

			// Emit three signals across two keys with spaced timestamps so order is deterministic.
			// SQLite only has second-level precision.
			step := bundle.driver.TimePrecision()
			if step < time.Second {
				step = time.Second
			}

			payloads := [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`)}
			keys := []string{"sig.a", "sig.b", "sig.a"}
			times := []time.Time{base, base.Add(step), base.Add(2 * step)}

			inserted := make([]*rivertype.WorkflowSignal, 3)
			for i := range 3 {
				var err error
				inserted[i], err = exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
					WorkflowID: "wf-005",
					SignalKey:  keys[i],
					Payload:    payloads[i],
					Now:        times[i],
				})
				require.NoError(t, err)
			}

			// List all signals for the workflow – should come back ordered by (created_at, id).
			all, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID: "wf-005",
				Max:        100,
			})
			require.NoError(t, err)
			require.Len(t, all, 3)
			require.Equal(t, inserted[0].ID, all[0].ID)
			require.Equal(t, inserted[1].ID, all[1].ID)
			require.Equal(t, inserted[2].ID, all[2].ID)

			// List with SignalKey filter → only "sig.a" rows (indexes 0 and 2)
			filtered, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID: "wf-005",
				SignalKey:  ptrutil.Ptr("sig.a"),
				Max:        100,
			})
			require.NoError(t, err)
			require.Len(t, filtered, 2)
			require.Equal(t, inserted[0].ID, filtered[0].ID)
			require.Equal(t, inserted[2].ID, filtered[1].ID)

			// Max limits results
			limited, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID: "wf-005",
				Max:        2,
			})
			require.NoError(t, err)
			require.Len(t, limited, 2)
		})
	})
}
