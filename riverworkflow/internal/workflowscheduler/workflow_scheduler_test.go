package workflowscheduler

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

// insertWorkflowTask inserts a workflow task row directly. State and metadata
// are set as given; finalized_at is left null. Returns the inserted row.
func insertWorkflowTask(ctx context.Context, t *testing.T, exec riverdriver.Executor, schema, workflowID, taskName string, deps []string, state rivertype.JobState, deadlineAt *time.Time) *rivertype.JobRow {
	t.Helper()

	meta := map[string]any{
		rivercommon.MetadataKeyWorkflowID:   workflowID,
		rivercommon.MetadataKeyWorkflowName: "scheduler-test",
		rivercommon.MetadataKeyWorkflowTask: taskName,
	}
	if len(deps) > 0 {
		meta[rivercommon.MetadataKeyWorkflowDeps] = deps
	}
	if deadlineAt != nil {
		meta[rivercommon.MetadataKeyWorkflowDeadlineAt] = deadlineAt.UTC().Format(time.RFC3339Nano)
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	now := time.Now()
	row, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		Kind:        "scheduler_test",
		MaxAttempts: 3,
		Metadata:    metaBytes,
		Priority:    1,
		Queue:       "default",
		ScheduledAt: &now,
		Schema:      schema,
		State:       state,
		Tags:        []string{},
	})
	require.NoError(t, err)
	return row
}

// TestCancelExpiredWorkflows asserts the scheduler's deadline-cancel path:
// any workflow with at least one non-terminal task past its deadline gets
// every non-terminal task cancelled, with the cancel reason stamped in
// metadata. Tasks of unrelated workflows must not be affected.
func TestCancelExpiredWorkflows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	exec := driver.GetExecutor()

	pastDeadline := time.Now().Add(-1 * time.Minute)
	futureDeadline := time.Now().Add(1 * time.Hour)

	// Workflow A: expired, has pending + scheduled tasks.
	wfExpired := "wf-expired"
	step1 := insertWorkflowTask(ctx, t, exec, schema, wfExpired, "step1", nil, rivertype.JobStateAvailable, &pastDeadline)
	step2 := insertWorkflowTask(ctx, t, exec, schema, wfExpired, "step2", []string{"step1"}, rivertype.JobStatePending, &pastDeadline)

	// Workflow B: future deadline — must NOT be cancelled.
	wfFuture := "wf-future"
	safeStep := insertWorkflowTask(ctx, t, exec, schema, wfFuture, "step1", nil, rivertype.JobStateAvailable, &futureDeadline)

	// Workflow C: no deadline metadata at all — must NOT be cancelled.
	wfNoDeadline := "wf-no-deadline"
	noDLStep := insertWorkflowTask(ctx, t, exec, schema, wfNoDeadline, "step1", nil, rivertype.JobStateAvailable, nil)

	scheduler := New(riversharedtest.BaseServiceArchetype(t), &Config{
		BatchSize: 100,
		Interval:  5 * time.Second,
		Schema:    schema,
	}, exec)
	scheduler.Logger = riversharedtest.Logger(t)

	require.NoError(t, scheduler.cancelExpiredWorkflows(ctx))

	// step1 and step2 of the expired workflow must now be cancelled with the
	// deadline reason stamped on their metadata.
	getRow := func(id int64) *rivertype.JobRow {
		t.Helper()
		// JobGetByID would be ideal; fall back to JobList with id filter.
		rows, err := exec.JobList(ctx, &riverdriver.JobListParams{
			Max:           10,
			OrderByClause: "id",
			Schema:        schema,
			WhereClause:   formatIDClause(id),
		})
		require.NoError(t, err)
		require.Len(t, rows, 1, "expected exactly one row for id=%d", id)
		return rows[0]
	}

	step1After := getRow(step1.ID)
	step2After := getRow(step2.ID)
	require.Equal(t, rivertype.JobStateCancelled, step1After.State, "step1 should be cancelled")
	require.Equal(t, rivertype.JobStateCancelled, step2After.State, "step2 should be cancelled")
	requireDeadlineReason(t, step1After)
	requireDeadlineReason(t, step2After)

	// Workflow B (future deadline) and C (no deadline) must be untouched.
	safeAfter := getRow(safeStep.ID)
	require.Equal(t, rivertype.JobStateAvailable, safeAfter.State,
		"workflow with a future deadline must NOT be cancelled")
	noDLAfter := getRow(noDLStep.ID)
	require.Equal(t, rivertype.JobStateAvailable, noDLAfter.State,
		"workflow without a deadline must NOT be cancelled")

}

func formatIDClause(id int64) string {
	return "id = " + strconv.FormatInt(id, 10)
}

func requireDeadlineReason(t *testing.T, row *rivertype.JobRow) {
	t.Helper()
	var meta map[string]any
	require.NoError(t, json.Unmarshal(row.Metadata, &meta))
	require.Equal(t, "workflow deadline exceeded", meta["river:workflow_cancel_reason"],
		"task should carry the deadline-cancel reason")
}
