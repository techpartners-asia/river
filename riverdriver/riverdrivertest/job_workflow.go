package riverdrivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

// workflowJobOpts carries options for inserting a workflow task via insertWorkflowJob.
type workflowJobOpts struct {
	DeadlineAt          time.Time // if non-zero, sets river:workflow_deadline_at (RFC3339Nano UTC)
	Deps                []string
	IgnoreCancelledDeps bool
	IgnoreDeletedDeps   bool
	IgnoreDiscardedDeps bool
	ScheduledAt         time.Time
	State               rivertype.JobState
	TaskName            string
	Wait                json.RawMessage
	WorkflowID          string
}

func insertWorkflowJob(ctx context.Context, t *testing.T, exec riverdriver.Executor, opts workflowJobOpts) *rivertype.JobRow {
	t.Helper()

	metadata := map[string]any{
		rivercommon.MetadataKeyWorkflowID:   opts.WorkflowID,
		rivercommon.MetadataKeyWorkflowTask: opts.TaskName,
	}
	if len(opts.Deps) > 0 {
		metadata[rivercommon.MetadataKeyWorkflowDeps] = opts.Deps
	}
	if opts.IgnoreCancelledDeps {
		metadata[rivercommon.MetadataKeyWorkflowIgnoreCancelledDeps] = true
	}
	if opts.IgnoreDiscardedDeps {
		metadata[rivercommon.MetadataKeyWorkflowIgnoreDiscardedDeps] = true
	}
	if opts.IgnoreDeletedDeps {
		metadata[rivercommon.MetadataKeyWorkflowIgnoreDeletedDeps] = true
	}
	if !opts.DeadlineAt.IsZero() {
		metadata[rivercommon.MetadataKeyWorkflowDeadlineAt] = opts.DeadlineAt.UTC().Format(time.RFC3339Nano)
	}
	if opts.Wait != nil {
		metadata[rivercommon.MetadataKeyWorkflowWait] = opts.Wait
	}
	metadataBytes, err := json.Marshal(metadata)
	require.NoError(t, err)

	jobOpts := &testfactory.JobOpts{
		Metadata: metadataBytes,
		State:    ptrutil.Ptr(opts.State),
	}
	if !opts.ScheduledAt.IsZero() {
		jobOpts.ScheduledAt = ptrutil.Ptr(opts.ScheduledAt)
	}
	return testfactory.Job(ctx, t, exec, jobOpts)
}

func exerciseJobCancelWorkflow[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()
		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("JobCancelWorkflow", func(t *testing.T) {
		t.Parallel()

		t.Run("CancelsNonFinalizedTasks", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			workflowID := "wf-cancel"
			completed := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
			pending := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", State: rivertype.JobStatePending, Deps: []string{"a"}})

			cancelled, err := exec.JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
				CancelAttemptedAt: time.Now(),
				ControlTopic:      "river_control",
				Now:               time.Now(),
				Reason:            "user requested",
				WorkflowID:        workflowID,
			})
			require.NoError(t, err)
			require.Len(t, cancelled, 1)
			require.Equal(t, pending.ID, cancelled[0].ID)
			require.Equal(t, rivertype.JobStateCancelled, cancelled[0].State)
			require.NotNil(t, cancelled[0].FinalizedAt)

			row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: completed.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, row.State)
		})

		t.Run("LeavesRunningTasksRunning", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			workflowID := "wf-cancel-running"
			running := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "a",
				State:      rivertype.JobStateRunning,
			})

			cancelled, err := exec.JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
				CancelAttemptedAt: time.Now(),
				ControlTopic:      "river_control",
				Now:               time.Now(),
				Reason:            "abort",
				WorkflowID:        workflowID,
			})
			require.NoError(t, err)
			require.Len(t, cancelled, 1)
			require.Equal(t, running.ID, cancelled[0].ID)
			require.Equal(t, rivertype.JobStateRunning, cancelled[0].State, "running tasks must stay running so the worker context can cancel")

			var meta map[string]any
			require.NoError(t, json.Unmarshal(cancelled[0].Metadata, &meta))
			require.Contains(t, meta, "cancel_attempted_at", "running task must carry cancel_attempted_at so the rescuer doesn't rescue it")
			require.IsType(t, "", meta["cancel_attempted_at"], "cancel_attempted_at must be a JSON string")
		})
	})
}

func exerciseJobRetryWorkflow[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()
		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("JobRetryWorkflow", func(t *testing.T) {
		t.Parallel()

		t.Run("FailedAndDownstream_ResetsCancelledAndDiscarded", func(t *testing.T) {
			t.Parallel()
			exec := setup(ctx, t)
			wfID := "wf-retry-fad"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: wfID, TaskName: "a", State: rivertype.JobStateDiscarded})
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: wfID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStateCancelled})
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: wfID, TaskName: "c", State: rivertype.JobStateCompleted})

			rows, err := exec.JobRetryWorkflow(ctx, &riverdriver.JobRetryWorkflowParams{
				Mode: "failed_and_downstream", Now: time.Now(), WorkflowID: wfID,
			})
			require.NoError(t, err)
			require.Len(t, rows, 2) // a and b
			for _, r := range rows {
				require.NotEqual(t, rivertype.JobStateCancelled, r.State)
				require.NotEqual(t, rivertype.JobStateDiscarded, r.State)
				require.Nil(t, r.FinalizedAt)
				require.Equal(t, 0, r.Attempt)
			}
		})

		t.Run("FailedOnly_ResetsOnlyDiscarded", func(t *testing.T) {
			t.Parallel()
			exec := setup(ctx, t)
			wfID := "wf-retry-fo"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: wfID, TaskName: "a", State: rivertype.JobStateDiscarded})
			cancelled := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: wfID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStateCancelled})

			rows, err := exec.JobRetryWorkflow(ctx, &riverdriver.JobRetryWorkflowParams{
				Mode: "failed_only", Now: time.Now(), WorkflowID: wfID,
			})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.NotEqual(t, cancelled.ID, rows[0].ID)
		})

		t.Run("All_ResetsEvenCompleted", func(t *testing.T) {
			t.Parallel()
			exec := setup(ctx, t)
			wfID := "wf-retry-all"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: wfID, TaskName: "a", State: rivertype.JobStateCompleted})
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: wfID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStateCompleted})

			rows, err := exec.JobRetryWorkflow(ctx, &riverdriver.JobRetryWorkflowParams{
				Mode: "all", Now: time.Now(), WorkflowID: wfID,
			})
			require.NoError(t, err)
			require.Len(t, rows, 2)
		})
	})
}

func exerciseJobGetWorkflowTasks[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()
		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("JobGetWorkflowTasks", func(t *testing.T) {
		t.Parallel()

		t.Run("ReturnsAllTasksForWorkflow", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			workflowID := "wf-get-all"
			a := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
			b := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", State: rivertype.JobStateAvailable, Deps: []string{"a"}})
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: "other-wf", TaskName: "a", State: rivertype.JobStateCompleted})

			rows, err := exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
				WorkflowID: workflowID,
			})
			require.NoError(t, err)
			require.Len(t, rows, 2)
			ids := []int64{rows[0].ID, rows[1].ID}
			require.ElementsMatch(t, []int64{a.ID, b.ID}, ids)
		})

		t.Run("FiltersByTaskNames", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			workflowID := "wf-filter"
			a := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", State: rivertype.JobStateAvailable})

			rows, err := exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
				WorkflowID: workflowID,
				TaskNames:  []string{"a"},
			})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, a.ID, rows[0].ID)
		})
	})
}

func exerciseJobApplyWorkflowWait[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()
		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("JobApplyWorkflowWait", func(t *testing.T) {
		t.Parallel()

		t.Run("PromotePastScheduledAt_BecomesAvailable", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			waitJob := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID:  "wf-wait-promote-past",
				TaskName:    "w1",
				State:       rivertype.JobStatePending,
				ScheduledAt: now.Add(-time.Hour), // scheduled in the past
				Wait:        json.RawMessage(`{"type":"duration","duration":"1h"}`),
			})

			row, err := exec.JobApplyWorkflowWait(ctx, &riverdriver.JobApplyWorkflowWaitParams{
				ID:      waitJob.ID,
				Outcome: "promote",
				Now:     now,
			})
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, rivertype.JobStateAvailable, row.State)

			var meta map[string]any
			require.NoError(t, json.Unmarshal(row.Metadata, &meta))
			require.Contains(t, meta, rivercommon.MetadataKeyWorkflowWaitResolvedAt, "resolved_at must be set on promote")
		})

		t.Run("PromoteFutureScheduledAt_BecomesScheduled", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			waitJob := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID:  "wf-wait-promote-future",
				TaskName:    "w2",
				State:       rivertype.JobStatePending,
				ScheduledAt: now.Add(time.Hour), // scheduled in the future
				Wait:        json.RawMessage(`{"type":"duration","duration":"1h"}`),
			})

			row, err := exec.JobApplyWorkflowWait(ctx, &riverdriver.JobApplyWorkflowWaitParams{
				ID:      waitJob.ID,
				Outcome: "promote",
				Now:     now,
			})
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, rivertype.JobStateScheduled, row.State)

			var meta map[string]any
			require.NoError(t, json.Unmarshal(row.Metadata, &meta))
			require.Contains(t, meta, rivercommon.MetadataKeyWorkflowWaitResolvedAt, "resolved_at must be set on promote")
		})

		t.Run("UnknownOutcome_ReturnsError", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			waitJob := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: "wf-wait-unknown-outcome",
				TaskName:   "w5",
				State:      rivertype.JobStatePending,
				Wait:       json.RawMessage(`{"type":"duration","duration":"1h"}`),
			})

			row, err := exec.JobApplyWorkflowWait(ctx, &riverdriver.JobApplyWorkflowWaitParams{
				ID:      waitJob.ID,
				Outcome: "bogus",
				Now:     now,
			})
			require.Error(t, err, "unknown outcome must return an error")
			require.Nil(t, row)

			// Confirm the row is unchanged — still pending.
			unchanged, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: waitJob.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, unchanged.State)
		})

		t.Run("Cancel_BecomesCancelledWithFinalizedAt", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			waitJob := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: "wf-wait-cancel",
				TaskName:   "w3",
				State:      rivertype.JobStatePending,
				Wait:       json.RawMessage(`{"type":"duration","duration":"1h"}`),
			})

			row, err := exec.JobApplyWorkflowWait(ctx, &riverdriver.JobApplyWorkflowWaitParams{
				ID:      waitJob.ID,
				Outcome: "cancel",
				Now:     now,
			})
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, rivertype.JobStateCancelled, row.State)
			require.NotNil(t, row.FinalizedAt)

			var meta map[string]any
			require.NoError(t, json.Unmarshal(row.Metadata, &meta))
			require.Contains(t, meta, rivercommon.MetadataKeyWorkflowWaitFailedReason, "failed_reason must be set on cancel")
		})

		t.Run("NonPendingRow_ReturnsErrNotFound", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			// Insert a job in available state (not pending) — outcome must not apply.
			availableJob := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: "wf-wait-not-pending",
				TaskName:   "w4",
				State:      rivertype.JobStateAvailable,
				Wait:       json.RawMessage(`{"type":"duration","duration":"1h"}`),
			})

			row, err := exec.JobApplyWorkflowWait(ctx, &riverdriver.JobApplyWorkflowWaitParams{
				ID:      availableJob.ID,
				Outcome: "promote",
				Now:     now,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound, "non-pending row must return ErrNotFound")
			require.Nil(t, row)
		})
	})
}

func exerciseJobGetWorkflowDeadlineExpired[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()
		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("JobGetWorkflowDeadlineExpired", func(t *testing.T) {
		t.Parallel()

		t.Run("ReturnsExpiredNonTerminalTasks", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			now := time.Now().UTC()
			workflowID := "wf-deadline-basic"

			// (a) A non-terminal task with a deadline in the PAST — must be returned.
			pastDeadlineTask := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "past-deadline",
				State:      rivertype.JobStateAvailable,
				DeadlineAt: now.Add(-time.Hour),
			})

			// (b) A non-terminal task with a deadline in the FUTURE — must NOT be returned.
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "future-deadline",
				State:      rivertype.JobStateAvailable,
				DeadlineAt: now.Add(time.Hour),
			})

			// (c) A non-workflow task (no deadline key) — must NOT be returned.
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "no-deadline",
				State:      rivertype.JobStateAvailable,
			})

			// (d) A finalized/completed task with a past deadline — must NOT be returned
			// because its state is not in the non-terminal set.
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "completed-past-deadline",
				State:      rivertype.JobStateCompleted,
				DeadlineAt: now.Add(-time.Hour),
			})

			// (e) A cancelled task with a past deadline — must NOT be returned.
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "cancelled-past-deadline",
				State:      rivertype.JobStateCancelled,
				DeadlineAt: now.Add(-time.Hour),
			})

			// (f) A discarded task with a past deadline — must NOT be returned.
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "discarded-past-deadline",
				State:      rivertype.JobStateDiscarded,
				DeadlineAt: now.Add(-time.Hour),
			})

			rows, err := exec.JobGetWorkflowDeadlineExpired(ctx, &riverdriver.JobGetWorkflowDeadlineExpiredParams{
				Now: now,
				Max: 100,
			})
			require.NoError(t, err)
			require.Len(t, rows, 1, "only the non-terminal task with a past deadline must be returned")
			require.Equal(t, pastDeadlineTask.ID, rows[0].ID)
		})

		t.Run("BoundarySubSecondPrecision", func(t *testing.T) {
			t.Parallel()

			// This test is the critical cross-dialect regression guard. It verifies
			// that a deadline stored as a whole-second RFC3339Nano string (no
			// fractional part, e.g. "...T05:00:00Z") is correctly returned when
			// `now` is that same second plus 1ms (e.g. "...T05:00:00.001Z").
			// A naive lexical string comparison would fail here because 'Z' (ASCII
			// 90) > '.' (ASCII 46), making the whole-second string sort AFTER the
			// sub-second one, causing missed deadlines. Postgres uses ::timestamptz
			// (correct). SQLite uses julianday() (correct). This test catches
			// regressions to a string-based SQLite approach.
			exec := setup(ctx, t)

			workflowID := "wf-deadline-boundary"

			// Deadline at a whole second — stored as RFC3339Nano with no fractional part.
			wholeSecond := time.Date(2020, 1, 1, 0, 0, 5, 0, time.UTC)

			// "now" is the same second + 1ms: chronologically after the deadline.
			nowWithSubSec := wholeSecond.Add(time.Millisecond)

			task := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "whole-second-deadline",
				State:      rivertype.JobStateAvailable,
				DeadlineAt: wholeSecond,
			})

			rows, err := exec.JobGetWorkflowDeadlineExpired(ctx, &riverdriver.JobGetWorkflowDeadlineExpiredParams{
				Now: nowWithSubSec,
				Max: 100,
			})
			require.NoError(t, err)
			require.Len(t, rows, 1, "whole-second deadline < sub-second now must be returned (boundary precision test)")
			require.Equal(t, task.ID, rows[0].ID)
		})

		t.Run("AfterIDFiltering", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			now := time.Now().UTC()
			workflowID := "wf-deadline-after-id"

			// Insert two non-terminal tasks with past deadlines. IDs are
			// auto-assigned in ascending order.
			firstTask := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "expired-first",
				State:      rivertype.JobStateAvailable,
				DeadlineAt: now.Add(-time.Hour),
			})
			secondTask := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "expired-second",
				State:      rivertype.JobStateAvailable,
				DeadlineAt: now.Add(-time.Hour),
			})

			// With AfterID = first task's id, only the second task should be returned.
			rows, err := exec.JobGetWorkflowDeadlineExpired(ctx, &riverdriver.JobGetWorkflowDeadlineExpiredParams{
				AfterID: firstTask.ID,
				Now:     now,
				Max:     100,
			})
			require.NoError(t, err)
			require.Len(t, rows, 1, "only the task with id > AfterID must be returned")
			require.Equal(t, secondTask.ID, rows[0].ID)

			// With AfterID = 0 (default), both tasks should be returned.
			rows, err = exec.JobGetWorkflowDeadlineExpired(ctx, &riverdriver.JobGetWorkflowDeadlineExpiredParams{
				AfterID: 0,
				Now:     now,
				Max:     100,
			})
			require.NoError(t, err)
			require.Len(t, rows, 2, "both expired tasks must be returned when AfterID=0")
		})
	})
}

func exerciseJobGetWorkflowWaitTasks[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()
		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("JobGetWorkflowWaitTasks", func(t *testing.T) {
		t.Parallel()

		t.Run("ReturnsPendingWaitTasks", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			workflowID := "wf-wait-list"

			// A pending task WITH the river:workflow_wait key — must be returned.
			waitTask := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "wait",
				State:      rivertype.JobStatePending,
				Wait:       json.RawMessage(`{"terms":[],"expr":"true"}`),
			})

			// A pending task WITHOUT the river:workflow_wait key — must NOT be returned.
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "no-wait",
				State:      rivertype.JobStatePending,
			})

			// A non-pending task WITH the river:workflow_wait key — must NOT be returned.
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "wait-available",
				State:      rivertype.JobStateAvailable,
				Wait:       json.RawMessage(`{"terms":[],"expr":"true"}`),
			})

			rows, err := exec.JobGetWorkflowWaitTasks(ctx, &riverdriver.JobGetWorkflowWaitTasksParams{
				Max: 100,
			})
			require.NoError(t, err)
			require.Len(t, rows, 1, "only the pending wait-bearing task must be returned")
			require.Equal(t, waitTask.ID, rows[0].ID)
			require.Equal(t, rivertype.JobStatePending, rows[0].State)
		})

		t.Run("AfterIDFiltering", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			workflowID := "wf-wait-after-id"

			// Insert two pending wait tasks. IDs are auto-assigned in ascending order.
			firstWaitTask := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "wait-first",
				State:      rivertype.JobStatePending,
				Wait:       json.RawMessage(`{"terms":[],"expr":"true"}`),
			})
			secondWaitTask := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "wait-second",
				State:      rivertype.JobStatePending,
				Wait:       json.RawMessage(`{"terms":[],"expr":"true"}`),
			})

			// With AfterID = first task's id, only the second task should be returned.
			rows, err := exec.JobGetWorkflowWaitTasks(ctx, &riverdriver.JobGetWorkflowWaitTasksParams{
				AfterID: firstWaitTask.ID,
				Max:     100,
			})
			require.NoError(t, err)
			require.Len(t, rows, 1, "only the task with id > AfterID must be returned")
			require.Equal(t, secondWaitTask.ID, rows[0].ID)

			// With AfterID = 0 (default), both tasks should be returned.
			rows, err = exec.JobGetWorkflowWaitTasks(ctx, &riverdriver.JobGetWorkflowWaitTasksParams{
				AfterID: 0,
				Max:     100,
			})
			require.NoError(t, err)
			require.Len(t, rows, 2, "both pending wait tasks must be returned when AfterID=0")
		})
	})
}

func exerciseJobUpdateWorkflowReady[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()
		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("JobUpdateWorkflowReady", func(t *testing.T) {
		t.Parallel()

		t.Run("PromotesWhenAllDepsCompleted", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			workflowID := "wf-promotes"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Len(t, updated, 1)
			require.Equal(t, taskB.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateAvailable, updated[0].State)
		})

		t.Run("LeavesPendingWhenDepStillRunning", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			workflowID := "wf-running-dep"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateRunning})
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Empty(t, updated)

			row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: taskB.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, row.State)
		})

		t.Run("CancelsWhenDepDiscarded", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			workflowID := "wf-discarded"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateDiscarded})
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Len(t, updated, 1)
			require.Equal(t, taskB.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateCancelled, updated[0].State)
			require.NotNil(t, updated[0].FinalizedAt)
		})

		t.Run("HonorsIgnoreDiscardedDeps", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			workflowID := "wf-ignore-discarded"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateDiscarded})
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending, IgnoreDiscardedDeps: true})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Len(t, updated, 1)
			require.Equal(t, taskB.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateAvailable, updated[0].State)
		})

		t.Run("ScheduledWhenScheduledAtInFuture", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			workflowID := "wf-scheduled"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending, ScheduledAt: now.Add(time.Hour)})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Len(t, updated, 1)
			require.Equal(t, taskB.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateScheduled, updated[0].State)
		})

		t.Run("CancelsWhenDepMissingAndIgnoreDeletedFalse", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			workflowID := "wf-missing-dep"
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Len(t, updated, 1)
			require.Equal(t, taskB.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateCancelled, updated[0].State)
		})

		t.Run("LeavesPendingWhenDepIsRetryable", func(t *testing.T) {
			t.Parallel()
			exec := setup(ctx, t)
			now := time.Now()
			workflowID := "wf-retryable-dep"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateRetryable})
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Empty(t, updated)

			row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: taskB.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, row.State)
		})

		t.Run("HonorsIgnoreCancelledDeps", func(t *testing.T) {
			t.Parallel()
			exec := setup(ctx, t)
			now := time.Now()
			workflowID := "wf-ignore-cancelled"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCancelled})
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending, IgnoreCancelledDeps: true})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Len(t, updated, 1)
			require.Equal(t, taskB.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateAvailable, updated[0].State)
		})

		t.Run("HonorsIgnoreDeletedDepsTrue", func(t *testing.T) {
			t.Parallel()
			exec := setup(ctx, t)
			now := time.Now()
			workflowID := "wf-ignore-deleted"
			// Task "a" is never inserted (simulating deleted dep).
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "b", Deps: []string{"a"}, State: rivertype.JobStatePending, IgnoreDeletedDeps: true})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Len(t, updated, 1)
			require.Equal(t, taskB.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateAvailable, updated[0].State)
		})

		t.Run("SkipsWaitBearingTasks", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)
			now := time.Now()

			workflowID := "wf-wait-bearing"
			_ = insertWorkflowJob(ctx, t, exec, workflowJobOpts{WorkflowID: workflowID, TaskName: "a", State: rivertype.JobStateCompleted})
			// taskB has all deps satisfied but also carries a river:workflow_wait
			// spec — the promotion query must skip it entirely so the Go
			// scheduler can evaluate the wait condition later.
			taskB := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "b",
				Deps:       []string{"a"},
				State:      rivertype.JobStatePending,
				Wait:       json.RawMessage(`{"type":"duration","duration":"1h"}`),
			})
			// taskC is a non-wait sibling with the same completed dep. It must
			// be promoted to available, proving the skip predicate targets only
			// wait-bearing tasks and not all pending workflow tasks.
			taskC := insertWorkflowJob(ctx, t, exec, workflowJobOpts{
				WorkflowID: workflowID,
				TaskName:   "c",
				Deps:       []string{"a"},
				State:      rivertype.JobStatePending,
			})

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			// Only the non-wait sibling (taskC) must be promoted.
			require.Len(t, updated, 1, "only the non-wait sibling must be promoted")
			require.Equal(t, taskC.ID, updated[0].ID)
			require.Equal(t, rivertype.JobStateAvailable, updated[0].State)

			// The wait-bearing task must remain pending.
			row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: taskB.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, row.State, "wait-bearing task must remain pending")
		})
	})
}
