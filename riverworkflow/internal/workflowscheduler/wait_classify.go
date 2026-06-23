package workflowscheduler

import (
	"github.com/riverqueue/river/rivertype"
)

// DepStatus represents the aggregate classification of a task's dependency set.
type DepStatus int

const (
	// DepStatusSatisfied means every declared dependency is in a terminal
	// "success" state (completed, or cancelled/discarded/deleted with the
	// matching ignore flag set). The task is eligible for wait evaluation.
	DepStatusSatisfied DepStatus = iota

	// DepStatusPending means at least one dependency is still in a
	// non-terminal state. The task must remain pending.
	DepStatusPending

	// DepStatusFailed means at least one dependency reached a terminal
	// "failure" state (cancelled, discarded, or missing/deleted) without the
	// matching ignore flag. The wait task should be cancelled.
	DepStatusFailed
)

// classifyDeps mirrors the dep-classification logic in JobUpdateWorkflowReady's
// SQL (see riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql.go). It is
// pure and has no database access.
//
// Parameters:
//   - deps: the list of declared dependency task names for the candidate task.
//   - siblings: a name→row map of all siblings in the workflow (keyed by their
//     river:workflow_task metadata value). Siblings not in this map are
//     considered deleted.
//   - ignoreCancelled, ignoreDiscarded, ignoreDeleted: ignore flags already
//     resolved from the task's metadata (river:workflow_ignore_*).
//
// The SQL precedence order (fail first, then pending, then satisfied) is:
//  1. If any dep is cancelled (without ignore) or discarded (without ignore) → Failed.
//  2. If any dep is missing (deleted, without ignore) → Failed.
//  3. If any dep is still non-terminal → Pending.
//  4. Otherwise → Satisfied.
func classifyDeps(
	deps []string,
	siblings map[string]*rivertype.JobRow,
	ignoreCancelled bool,
	ignoreDiscarded bool,
	ignoreDeleted bool,
) DepStatus {
	// Terminal states that count as "done OK" without an explicit fail.
	hasPending := false
	hasFailed := false

	for _, dep := range deps {
		row, found := siblings[dep]
		if !found {
			// Dep is missing (deleted from the database).
			if !ignoreDeleted {
				// This is a hard failure — mirror fail when dep_rows_found < dep_rows_declared.
				hasFailed = true
			}
			// With ignoreDeleted, treat as satisfied; continue.
			continue
		}

		switch row.State {
		case rivertype.JobStateCompleted:
			// Always satisfied.

		case rivertype.JobStateCancelled:
			if !ignoreCancelled {
				hasFailed = true
			}

		case rivertype.JobStateDiscarded:
			if !ignoreDiscarded {
				hasFailed = true
			}

		default:
			// Non-terminal: available, pending, running, retryable, scheduled.
			hasPending = true
		}
	}

	// Precedence matches SQL: fail before pending before satisfied.
	// Note: even if both hasFailed and hasPending are set, Failed wins
	// (mirrors: "fail_cancelled OR fail_discarded" is checked before the
	// any-nonterminal check in the SQL's classified CTE).
	switch {
	case hasFailed:
		return DepStatusFailed
	case hasPending:
		return DepStatusPending
	default:
		return DepStatusSatisfied
	}
}
