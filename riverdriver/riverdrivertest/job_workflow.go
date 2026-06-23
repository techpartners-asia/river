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

			updated, err := exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{Max: 100, Now: now})
			require.NoError(t, err)
			require.Empty(t, updated, "wait-bearing task must not be promoted by the SQL promotion pass")

			row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: taskB.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, row.State, "wait-bearing task must remain pending")
		})
	})
}
