package workflowscheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

// makeRow is a tiny helper that builds a *rivertype.JobRow with just the fields
// classifyDeps reads (State, FinalizedAt).
func makeRow(state rivertype.JobState) *rivertype.JobRow {
	row := &rivertype.JobRow{State: state}
	if state == rivertype.JobStateCompleted ||
		state == rivertype.JobStateCancelled ||
		state == rivertype.JobStateDiscarded {
		now := time.Now()
		row.FinalizedAt = &now
	}
	return row
}

// TestClassifyDeps covers every branch of the dep-classifier.
func TestClassifyDeps(t *testing.T) {
	t.Parallel()

	t.Run("AllCompleted_Satisfied", func(t *testing.T) {
		t.Parallel()
		deps := []string{"a", "b"}
		siblings := map[string]*rivertype.JobRow{
			"a": makeRow(rivertype.JobStateCompleted),
			"b": makeRow(rivertype.JobStateCompleted),
		}
		status := classifyDeps(deps, siblings, false, false, false)
		require.Equal(t, DepStatusSatisfied, status)
	})

	t.Run("OneRunning_Pending", func(t *testing.T) {
		t.Parallel()
		deps := []string{"a", "b"}
		siblings := map[string]*rivertype.JobRow{
			"a": makeRow(rivertype.JobStateCompleted),
			"b": makeRow(rivertype.JobStateRunning),
		}
		status := classifyDeps(deps, siblings, false, false, false)
		require.Equal(t, DepStatusPending, status)
	})

	t.Run("DiscardedWithoutIgnore_Failed", func(t *testing.T) {
		t.Parallel()
		deps := []string{"a"}
		siblings := map[string]*rivertype.JobRow{
			"a": makeRow(rivertype.JobStateDiscarded),
		}
		status := classifyDeps(deps, siblings, false, false, false)
		require.Equal(t, DepStatusFailed, status)
	})

	t.Run("DiscardedWithIgnore_Satisfied", func(t *testing.T) {
		t.Parallel()
		deps := []string{"a"}
		siblings := map[string]*rivertype.JobRow{
			"a": makeRow(rivertype.JobStateDiscarded),
		}
		status := classifyDeps(deps, siblings, false, true, false)
		require.Equal(t, DepStatusSatisfied, status)
	})

	t.Run("MissingWithoutIgnoreDeleted_Failed", func(t *testing.T) {
		t.Parallel()
		// Dep "a" is declared but not present in siblings (deleted).
		deps := []string{"a"}
		siblings := map[string]*rivertype.JobRow{}
		status := classifyDeps(deps, siblings, false, false, false)
		require.Equal(t, DepStatusFailed, status)
	})

	t.Run("MissingWithIgnoreDeleted_Satisfied", func(t *testing.T) {
		t.Parallel()
		// Dep "a" is declared but not present in siblings (deleted), but
		// ignore_deleted is set so it should be treated as satisfied.
		deps := []string{"a"}
		siblings := map[string]*rivertype.JobRow{}
		status := classifyDeps(deps, siblings, false, false, true)
		require.Equal(t, DepStatusSatisfied, status)
	})

	t.Run("CancelledWithoutIgnore_Failed", func(t *testing.T) {
		t.Parallel()
		deps := []string{"a"}
		siblings := map[string]*rivertype.JobRow{
			"a": makeRow(rivertype.JobStateCancelled),
		}
		status := classifyDeps(deps, siblings, false, false, false)
		require.Equal(t, DepStatusFailed, status)
	})

	t.Run("CancelledWithIgnore_Satisfied", func(t *testing.T) {
		t.Parallel()
		deps := []string{"a"}
		siblings := map[string]*rivertype.JobRow{
			"a": makeRow(rivertype.JobStateCancelled),
		}
		status := classifyDeps(deps, siblings, true, false, false)
		require.Equal(t, DepStatusSatisfied, status)
	})

	t.Run("NoDeps_Satisfied", func(t *testing.T) {
		t.Parallel()
		// A wait-only task (no deps) — deps list is empty.
		status := classifyDeps([]string{}, map[string]*rivertype.JobRow{}, false, false, false)
		require.Equal(t, DepStatusSatisfied, status)
	})

	t.Run("FailedAndNonTerminal_FailedTakesPrecedence", func(t *testing.T) {
		t.Parallel()
		// When a dep is failed AND another is still running, Failed wins.
		// (Mirrors SQL: fail_cancelled/fail_discarded checked first.)
		deps := []string{"a", "b"}
		siblings := map[string]*rivertype.JobRow{
			"a": makeRow(rivertype.JobStateRunning),
			"b": makeRow(rivertype.JobStateCancelled),
		}
		status := classifyDeps(deps, siblings, false, false, false)
		require.Equal(t, DepStatusFailed, status)
	})
}
