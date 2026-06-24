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
			require.JSONEq(t, `{"item":"widget"}`, string(sig.Payload))
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

		t.Run("List_NewestOrderingRespectsTruncation", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			base := time.Now().UTC().Truncate(time.Second)

			// Use at least second-level spacing (SQLite has second-level precision).
			step := bundle.driver.TimePrecision()
			if step < time.Second {
				step = time.Second
			}

			// Insert 3 signals with deterministically increasing created_at.
			inserted := make([]*rivertype.WorkflowSignal, 3)
			for i := range 3 {
				var err error
				inserted[i], err = exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
					WorkflowID: "wf-006",
					SignalKey:  "sig.trunc",
					Payload:    []byte(`{}`),
					Now:        base.Add(time.Duration(i) * step),
				})
				require.NoError(t, err)
			}

			// Fetch only 2 rows with OrderByNewest:true — must return the 2 newest.
			got, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID:    "wf-006",
				Max:           2,
				OrderByNewest: true,
			})
			require.NoError(t, err)
			require.Len(t, got, 2)
			// First row must be the newest (inserted[2]), second must be inserted[1].
			require.Equal(t, inserted[2].ID, got[0].ID, "first result must be the newest signal (inserted[2])")
			require.Equal(t, inserted[1].ID, got[1].ID, "second result must be inserted[1]")
			// The oldest signal (inserted[0]) must not be present.
			for _, s := range got {
				require.NotEqual(t, inserted[0].ID, s.ID, "oldest signal must not appear in newest-2 result")
			}
		})

		t.Run("List_IncludeAfterResolution", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			base := time.Now().UTC().Truncate(time.Second)

			step := bundle.driver.TimePrecision()
			if step < time.Second {
				step = time.Second
			}

			// Insert two signals with different keys: one to be resolved, one to stay active.
			sigResolved, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID: "wf-resolved-01",
				SignalKey:  "sig.done",
				Payload:    []byte(`{"n":1}`),
				Now:        base,
			})
			require.NoError(t, err)

			sigActive, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID: "wf-resolved-01",
				SignalKey:  "sig.active",
				Payload:    []byte(`{"n":2}`),
				Now:        base.Add(step),
			})
			require.NoError(t, err)

			// Mark only "sig.done" as resolved.
			err = exec.WorkflowSignalMarkResolved(ctx, &riverdriver.WorkflowSignalMarkResolvedParams{
				WorkflowID: "wf-resolved-01",
				SignalKeys: []string{"sig.done"},
				Now:        base.Add(2 * step),
			})
			require.NoError(t, err)

			// Default list (IncludeResolved:false) must exclude the resolved signal and return only active.
			got, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID:      "wf-resolved-01",
				IncludeResolved: false,
				Max:             100,
			})
			require.NoError(t, err)
			require.Len(t, got, 1, "default filter must exclude resolved signal")
			require.Equal(t, sigActive.ID, got[0].ID, "only the unresolved signal must be returned")

			// With IncludeResolved:true both signals must appear.
			gotAll, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID:      "wf-resolved-01",
				IncludeResolved: true,
				Max:             100,
			})
			require.NoError(t, err)
			require.Len(t, gotAll, 2, "IncludeResolved:true must return all signals including resolved")

			// The resolved signal must have a non-nil resolved_at.
			for _, s := range gotAll {
				if s.ID == sigResolved.ID {
					require.NotNil(t, s.ResolvedAt, "resolved signal must have non-nil resolved_at")
				}
			}

			// Mark the same key a second time — must be idempotent (no error).
			err = exec.WorkflowSignalMarkResolved(ctx, &riverdriver.WorkflowSignalMarkResolvedParams{
				WorkflowID: "wf-resolved-01",
				SignalKeys: []string{"sig.done"},
				Now:        base.Add(3 * step),
			})
			require.NoError(t, err)
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

		t.Run("ListNewest_IncludeAfterResolution", func(t *testing.T) {
			t.Parallel()

			// Verifies the resolved_at filter works on the DESC (OrderByNewest:true)
			// path used by LatestForTask and the workflow scheduler. Mirrors
			// List_IncludeAfterResolution but uses two distinct keys and asserts
			// newest-first ordering so the test is meaningful for the DESC path.

			exec, bundle := setup(ctx, t)

			base := time.Now().UTC().Truncate(time.Second)

			step := bundle.driver.TimePrecision()
			if step < time.Second {
				step = time.Second
			}

			// Insert two signals with distinct keys: resolved one is older (base),
			// active one is newer (base+step) so newest-first ordering is testable.
			sigResolved, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID: "wf-newest-resolved-01",
				SignalKey:  "sig.done",
				Payload:    []byte(`{"n":1}`),
				Now:        base,
			})
			require.NoError(t, err)

			sigActive, err := exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
				WorkflowID: "wf-newest-resolved-01",
				SignalKey:  "sig.active",
				Payload:    []byte(`{"n":2}`),
				Now:        base.Add(step),
			})
			require.NoError(t, err)

			// Mark only "sig.done" as resolved.
			err = exec.WorkflowSignalMarkResolved(ctx, &riverdriver.WorkflowSignalMarkResolvedParams{
				WorkflowID: "wf-newest-resolved-01",
				SignalKeys: []string{"sig.done"},
				Now:        base.Add(2 * step),
			})
			require.NoError(t, err)

			// OrderByNewest:true, IncludeResolved:false — must exclude the resolved signal.
			got, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID:      "wf-newest-resolved-01",
				IncludeResolved: false,
				OrderByNewest:   true,
				Max:             100,
			})
			require.NoError(t, err)
			require.Len(t, got, 1, "DESC path with IncludeResolved:false must exclude the resolved signal")
			require.Equal(t, sigActive.ID, got[0].ID, "only the unresolved signal must be returned")

			// OrderByNewest:true, IncludeResolved:true — must include both, newest first.
			gotAll, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
				WorkflowID:      "wf-newest-resolved-01",
				IncludeResolved: true,
				OrderByNewest:   true,
				Max:             100,
			})
			require.NoError(t, err)
			require.Len(t, gotAll, 2, "DESC path with IncludeResolved:true must return both signals")
			// Newest (sigActive, base+step) must come first in DESC order.
			require.Equal(t, sigActive.ID, gotAll[0].ID, "newest signal must be first in DESC result")
			require.Equal(t, sigResolved.ID, gotAll[1].ID, "older resolved signal must be second in DESC result")

			// The resolved signal must have a non-nil resolved_at.
			for _, s := range gotAll {
				if s.ID == sigResolved.ID {
					require.NotNil(t, s.ResolvedAt, "resolved signal must have non-nil resolved_at")
				}
			}
		})
	})
}
