package riverworkflow_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/riverworkflow"
)

// TestSimulation_LoadSoak drives many mixed-wait workflows through TWO competing
// clients running against the same schema. Each client runs the full
// leader-elected maintenance stack (workflow scheduler + timer poller) and a
// pool of workers, so this exercises the concurrency-safety guards (state-keyed
// promote/cancel, signal mark-resolved, deadline scan pagination) under load:
// only one scheduler is leader at a time, jobs are fetched with SKIP LOCKED, and
// every task must reach completed exactly once with no stuck tasks.
//
// Each workflow is a mixed-wait DAG:
//
//	root ──► approve  (signal "go", gated on payload.ok) ──┐
//	     └─► cooldown (timer, fires shortly after wait)  ──┴─► tail
//
// Tune the volume with LOAD_WORKFLOWS (default 40). Skipped under `go test -short`.
func TestSimulation_LoadSoak(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("load/soak test; skipped in -short mode")
	}

	numWorkflows := 40
	if v := os.Getenv("LOAD_WORKFLOWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			numWorkflows = n
		}
	}
	const tasksPerWorkflow = 4
	totalTasks := numWorkflows * tasksPerWorkflow

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	// Two independent clients sharing the schema. Each gets its own worker pool;
	// both run the scheduler + timer poller and compete for leadership.
	newClient := func() *riverworkflow.Client[pgx.Tx] {
		w := &recordingWorker{mu: &sync.Mutex{}, dur: 0}
		workers := river.NewWorkers()
		river.AddWorker(workers, w)
		client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
			Config: river.Config{
				Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 16}},
				Schema:  schema,
				Workers: workers,
			},
			WorkflowScheduler: riverworkflow.WorkflowSchedulerConfig{
				Interval:                    50 * time.Millisecond,
				WorkflowTimerPollerInterval: 50 * time.Millisecond,
			},
		})
		require.NoError(t, err)
		return client
	}

	insertClient := newClient()
	secondClient := newClient()

	// Build + insert all workflows up front (before starting either client), so
	// the schedulers see the full backlog at once. Keep the handles to emit
	// signals later.
	workflows := make([]*riverworkflow.Workflow[pgx.Tx], 0, numWorkflows)
	for i := 0; i < numWorkflows; i++ {
		wf := insertClient.NewWorkflow(&riverworkflow.WorkflowOpts{
			Name: "loadsoak-" + strconv.Itoa(i),
		})
		wf.Add("root", recordingWorkerArgs{Task: "root"}, nil, nil)
		wf.Add("approve", recordingWorkerArgs{Task: "approve"}, nil, &riverworkflow.WorkflowTaskOpts{
			Deps: []string{"root"},
			Wait: &riverworkflow.WaitSpec{
				Terms: []riverworkflow.WaitTermSpec{
					riverworkflow.WaitTermSignal("go", "go", "payload.ok"),
				},
				Expr: "go",
			},
		})
		wf.Add("cooldown", recordingWorkerArgs{Task: "cooldown"}, nil, &riverworkflow.WorkflowTaskOpts{
			Deps: []string{"root"},
			Wait: &riverworkflow.WaitSpec{
				Terms: []riverworkflow.WaitTermSpec{
					riverworkflow.WaitTermTimer(
						riverworkflow.TimerAfterWaitStarted("cooldown", time.Second),
					),
				},
				Expr: "cooldown",
			},
		})
		wf.Add("tail", recordingWorkerArgs{Task: "tail"}, nil, &riverworkflow.WorkflowTaskOpts{
			Deps: []string{"approve", "cooldown"},
		})

		prep, err := wf.Prepare(ctx)
		require.NoError(t, err)
		_, err = insertClient.InsertMany(ctx, prep.Jobs)
		require.NoError(t, err)
		workflows = append(workflows, wf)
	}

	require.NoError(t, insertClient.Start(ctx))
	require.NoError(t, secondClient.Start(ctx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, insertClient.Stop(stopCtx))
		require.NoError(t, secondClient.Stop(stopCtx))
	}()

	// Emit the "go" signal to every workflow concurrently — a signal storm while
	// the schedulers are actively draining wait tasks.
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, wf := range workflows {
		wg.Add(1)
		sem <- struct{}{}
		go func(wf *riverworkflow.Workflow[pgx.Tx]) {
			defer wg.Done()
			defer func() { <-sem }()
			_, err := wf.Signals().Emit(ctx, "go", map[string]any{"ok": true}, nil)
			require.NoError(t, err)
		}(wf)
	}
	wg.Wait()

	// Poll the DB for completions across both clients (subscription events are
	// per-client, so polling state is the reliable cross-client signal). Every
	// task must reach completed, exactly once, with none stuck.
	completedCount := func() int {
		var n int
		err := dbPool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s.river_job WHERE state = 'completed'", schema),
		).Scan(&n)
		require.NoError(t, err)
		return n
	}
	nonTerminalCount := func() int {
		var n int
		err := dbPool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s.river_job WHERE finalized_at IS NULL", schema),
		).Scan(&n)
		require.NoError(t, err)
		return n
	}

	deadline := time.After(150 * time.Second)
	for {
		done := completedCount()
		if done >= totalTasks {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: %d/%d tasks completed, %d still non-terminal",
				done, totalTasks, nonTerminalCount())
		case <-time.After(250 * time.Millisecond):
		}
	}

	// Exactly the expected number of tasks, all terminal, none cancelled or
	// discarded (a stuck/mis-promoted task would show up as non-completed).
	require.Equal(t, totalTasks, completedCount(),
		"every task must complete exactly once")
	require.Zero(t, nonTerminalCount(), "no task may be left non-terminal")
}
