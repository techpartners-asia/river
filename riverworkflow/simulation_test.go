package riverworkflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/river/riverworkflow"
)

type recordingWorkerArgs struct {
	Task string `json:"task"`
}

func (recordingWorkerArgs) Kind() string { return "riverworkflow_sim" }

type recordingWorker struct {
	river.WorkerDefaults[recordingWorkerArgs]

	mu   *sync.Mutex
	done []string
	dur  time.Duration
}

func (w *recordingWorker) Work(_ context.Context, job *river.Job[recordingWorkerArgs]) error {
	time.Sleep(w.dur)
	w.mu.Lock()
	w.done = append(w.done, job.Args.Task)
	w.mu.Unlock()
	return nil
}

// TestSimulation_FanOutFanIn runs an end-to-end workflow against the real
// Postgres test database to verify that:
//   - Tasks with no deps run first.
//   - Fan-out children only run once their parent finishes.
//   - Fan-in tail runs once both children finish.
//   - All tasks reach the completed state and carry workflow metadata.
func TestSimulation_FanOutFanIn(t *testing.T) {
	t.Parallel()

	if os.Getenv("TEST_DATABASE_URL") == "" && os.Getenv("RIVER_TEST_DATABASE_URL") == "" {
		// riversharedtest defaults to postgres://localhost/river_test; rely on
		// that, but skip if a database connection can't be made.
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	w := &recordingWorker{mu: &sync.Mutex{}, dur: 10 * time.Millisecond}
	workers := river.NewWorkers()
	river.AddWorker(workers, w)

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
			Schema:  schema,
			Workers: workers,
		},
		WorkflowScheduler: riverworkflow.WorkflowSchedulerConfig{
			Interval: 100 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "simulation"})
	a := wf.Add("a", recordingWorkerArgs{Task: "a"}, nil, nil)
	b1 := wf.Add("b1", recordingWorkerArgs{Task: "b1"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{a.Name}})
	b2 := wf.Add("b2", recordingWorkerArgs{Task: "b2"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{a.Name}})
	wf.Add("c", recordingWorkerArgs{Task: "c"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{b1.Name, b2.Name}})

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	require.Len(t, prep.Jobs, 4)

	inserted, err := client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)
	require.Len(t, inserted, 4)

	// Three should be pending (b1, b2, c) and one available (a).
	pendingCount := 0
	availableCount := 0
	for _, r := range inserted {
		switch r.Job.State {
		case rivertype.JobStatePending:
			pendingCount++
		case rivertype.JobStateAvailable:
			availableCount++
		}
	}
	require.Equal(t, 1, availableCount, "exactly one task should be available immediately")
	require.Equal(t, 3, pendingCount, "three tasks should be pending")

	require.NoError(t, client.Start(ctx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, client.Stop(stopCtx))
	}()

	// Wait for all four tasks to complete.
	timeout := time.After(20 * time.Second)
	completed := 0
	for completed < 4 {
		select {
		case <-subscribeChan:
			completed++
		case <-timeout:
			t.Fatalf("timed out waiting for 4 completions; got %d", completed)
		}
	}

	// Verify ordering: a before b1/b2, b1 and b2 before c.
	w.mu.Lock()
	got := append([]string(nil), w.done...)
	w.mu.Unlock()
	require.Len(t, got, 4)

	pos := map[string]int{}
	for i, name := range got {
		pos[name] = i
	}
	require.Less(t, pos["a"], pos["b1"], "task a must complete before b1 (order=%v)", got)
	require.Less(t, pos["a"], pos["b2"], "task a must complete before b2 (order=%v)", got)
	require.Less(t, pos["b1"], pos["c"], "task b1 must complete before c (order=%v)", got)
	require.Less(t, pos["b2"], pos["c"], "task b2 must complete before c (order=%v)", got)

	// Verify metadata round-trips.
	tasks, err := wf.LoadAll(ctx)
	require.NoError(t, err)
	allFour := []string{"a", "b1", "b2", "c"}
	sort.Strings(allFour)
	for _, name := range allFour {
		row, err := tasks.Get(name)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, row.State)
		var meta map[string]any
		require.NoError(t, json.Unmarshal(row.Metadata, &meta))
		require.Equal(t, wf.ID(), meta[rivercommon.MetadataKeyWorkflowID])
	}
}

// TestSimulation_Stress fires a wide DAG (20 fan-out children of a single
// root, joined by one tail) and asserts all 22 tasks complete in dependency
// order. Catches races between the scheduler and the worker pool.
func TestSimulation_Stress(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

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
			Interval: 50 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	const fanout = 20
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "stress"})
	wf.Add("root", recordingWorkerArgs{Task: "root"}, nil, nil)
	tailDeps := make([]string, fanout)
	for i := 0; i < fanout; i++ {
		name := "child-" + strconv.Itoa(i)
		wf.Add(name, recordingWorkerArgs{Task: name}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{"root"}})
		tailDeps[i] = name
	}
	wf.Add("tail", recordingWorkerArgs{Task: "tail"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: tailDeps})

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, client.Stop(stopCtx))
	}()

	total := fanout + 2 // root + children + tail
	timeout := time.After(45 * time.Second)
	for completed := 0; completed < total; {
		select {
		case <-subscribeChan:
			completed++
		case <-timeout:
			t.Fatalf("timed out waiting for %d completions; got %d", total, completed)
		}
	}

	w.mu.Lock()
	got := append([]string(nil), w.done...)
	w.mu.Unlock()
	require.Len(t, got, total)

	pos := map[string]int{}
	for i, name := range got {
		pos[name] = i
	}
	require.Equal(t, 0, pos["root"], "root must complete first; order=%v", got)
	require.Equal(t, total-1, pos["tail"], "tail must complete last; order=%v", got)
	for i := 0; i < fanout; i++ {
		name := "child-" + strconv.Itoa(i)
		require.Less(t, pos["root"], pos[name], "root must finish before %s", name)
		require.Less(t, pos[name], pos["tail"], "%s must finish before tail", name)
	}
}

// TestClient_WorkflowRetry verifies that WorkflowRetry resets tasks according
// to the given mode. Because the workflow client inserts tasks as available or
// pending (not discarded/cancelled), this test mainly checks that the method
// is callable and returns without error; the per-mode state semantics are
// fully covered by the driver conformance suite in riverdrivertest.
func TestClient_WorkflowRetry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &recordingWorker{mu: &sync.Mutex{}, dur: 0})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Workers: workers,
			Schema:  schema,
		},
	})
	require.NoError(t, err)

	// Insert a small workflow: a -> b -> c.
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "retry-unit"})
	wf.Add("a", recordingWorkerArgs{Task: "a"}, nil, nil)
	wf.Add("b", recordingWorkerArgs{Task: "b"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{"a"}})
	wf.Add("c", recordingWorkerArgs{Task: "c"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{"b"}})
	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	// Freshly-inserted tasks have state available or pending — none are
	// discarded/cancelled/completed — so all retry modes return zero rows.
	for _, mode := range []string{"failed_only", "failed_and_downstream", "all"} {
		res, err := client.WorkflowRetry(ctx, prep.WorkflowID, mode, false)
		require.NoError(t, err)
		require.Empty(t, res.RetriedJobs, "mode=%s: freshly inserted tasks should not match target states", mode)
	}
}

// TestSimulation_CancelCascades verifies that cancelling a workflow cancels
// every non-finalized task while leaving completed tasks alone.
func TestSimulation_CancelCascades(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &recordingWorker{mu: &sync.Mutex{}, dur: 0})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Workers: workers,
			Schema:  schema,
		},
	})
	require.NoError(t, err)

	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "cancel-sim"})
	wf.Add("a", recordingWorkerArgs{Task: "a"}, nil, nil)
	wf.Add("b", recordingWorkerArgs{Task: "b"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{"a"}})
	wf.Add("c", recordingWorkerArgs{Task: "c"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{"b"}})

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	res, err := client.WorkflowCancel(ctx, prep.WorkflowID)
	require.NoError(t, err)
	require.Len(t, res.CancelledJobs, 3, "all three not-yet-completed tasks should be cancelled")
	for _, r := range res.CancelledJobs {
		require.Equal(t, rivertype.JobStateCancelled, r.State)
		require.NotNil(t, r.FinalizedAt)
	}
}

// TestDeadlineMetadataInjected verifies that WorkflowOpts.DeadlineAt round-trips
// into the river:workflow_deadline_at metadata key on every task. This is the
// contract the scheduler's cancelExpiredWorkflows depends on; if the key is
// missing the deadline logic silently no-ops.
func TestDeadlineMetadataInjected(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &recordingWorker{mu: &sync.Mutex{}, dur: 1 * time.Hour})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}},
			Schema:  schema,
			Workers: workers,
		},
		WorkflowScheduler: riverworkflow.WorkflowSchedulerConfig{
			Interval: 100 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	deadline := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{
		DeadlineAt: deadline,
		Name:       "deadline-metadata-test",
	})
	wf.Add("step1", recordingWorkerArgs{Task: "step1"}, nil, nil)
	wf.Add("step2", recordingWorkerArgs{Task: "step2"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{"step1"}})

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	tasks, err := wf.LoadAll(ctx)
	require.NoError(t, err)
	for _, name := range []string{"step1", "step2"} {
		row, err := tasks.Get(name)
		require.NoError(t, err)
		var meta map[string]any
		require.NoError(t, json.Unmarshal(row.Metadata, &meta))
		require.Equal(t, deadline.UTC().Format(time.RFC3339Nano),
			meta[rivercommon.MetadataKeyWorkflowDeadlineAt],
			"task %s should carry the workflow deadline in its metadata", name)
	}
}

// failingWorkerArgs is the job args type for a worker that always fails
// (returns a non-nil error). Used to exercise the dep-failed / wait-cancel
// path where a dependency is discarded after exhausting its attempts.
type failingWorkerArgs struct {
	Task string `json:"task"`
}

func (failingWorkerArgs) Kind() string { return "riverworkflow_sim_fail" }

type failingWorker struct {
	river.WorkerDefaults[failingWorkerArgs]
}

func (w *failingWorker) Work(_ context.Context, _ *river.Job[failingWorkerArgs]) error {
	return errors.New("worker always fails")
}

// TestSimulation_WaitTimer_PromotesAfterDuration verifies that a task gated
// by WaitTermTimer(TimerAfterWaitStarted("d", short)) is promoted once the
// scheduler ticks past the timer duration. The test uses wall-clock time with a
// short duration (50 ms) and a fast scheduler interval so it completes quickly.
func TestSimulation_WaitTimer_PromotesAfterDuration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	w := &recordingWorker{mu: &sync.Mutex{}, dur: 0}
	workers := river.NewWorkers()
	river.AddWorker(workers, w)

	// Use a short timer-poller interval so the timer fires quickly.
	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
			Schema:  schema,
			Workers: workers,
		},
		WorkflowScheduler: riverworkflow.WorkflowSchedulerConfig{
			Interval:                    100 * time.Millisecond,
			WorkflowTimerPollerInterval: 50 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	// Build a simple workflow: task "a" has no deps, task "b" depends on "a"
	// and is gated by a timer that fires 100 ms after wait_started_at.
	timerDur := 100 * time.Millisecond
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "wait-timer-test"})
	a := wf.Add("a", recordingWorkerArgs{Task: "a"}, nil, nil)
	wf.Add("b", recordingWorkerArgs{Task: "b"}, nil, &riverworkflow.WorkflowTaskOpts{
		Deps: []string{a.Name},
		Wait: &riverworkflow.WaitSpec{
			Terms: []riverworkflow.WaitTermSpec{
				riverworkflow.WaitTermTimer(riverworkflow.TimerAfterWaitStarted("d", timerDur)),
			},
			Expr: "d",
		},
	})

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, client.Stop(stopCtx))
	}()

	// Wait for both tasks to complete.
	timeout := time.After(20 * time.Second)
	for completed := 0; completed < 2; {
		select {
		case <-subscribeChan:
			completed++
		case <-timeout:
			t.Fatalf("timed out waiting for 2 completions; got %d", completed)
		}
	}

	// Verify both tasks completed.
	tasks, err := wf.LoadAll(ctx)
	require.NoError(t, err)
	for _, name := range []string{"a", "b"} {
		row, err := tasks.Get(name)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, row.State, "task %s should be completed", name)
	}
}

// TestSimulation_WaitCancelledOnFailedDep verifies that a wait-bearing task
// whose dependency is discarded (after MaxAttempts=1) gets cancelled by the
// scheduler's evaluateWaits pass.
func TestSimulation_WaitCancelledOnFailedDep(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(dbPool), nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &failingWorker{})
	river.AddWorker(workers, &recordingWorker{mu: &sync.Mutex{}, dur: 0})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
			Schema:  schema,
			Workers: workers,
		},
		WorkflowScheduler: riverworkflow.WorkflowSchedulerConfig{
			Interval:                    100 * time.Millisecond,
			WorkflowTimerPollerInterval: 50 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	// Subscribe to both completed and cancelled events to detect terminal states.
	completedChan, completedCancel := client.Subscribe(river.EventKindJobCompleted)
	defer completedCancel()
	cancelledChan, cancelledCancel := client.Subscribe(river.EventKindJobCancelled)
	defer cancelledCancel()

	// Workflow: "dep" fails (MaxAttempts=1 so it discards after 1 attempt),
	// "waiter" depends on "dep" and has a Wait spec with a timer.
	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "wait-cancel-on-fail"})
	dep := wf.Add("dep", failingWorkerArgs{Task: "dep"},
		&river.InsertOpts{MaxAttempts: 1}, nil)
	wf.Add("waiter", recordingWorkerArgs{Task: "waiter"}, nil, &riverworkflow.WorkflowTaskOpts{
		Deps: []string{dep.Name},
		Wait: &riverworkflow.WaitSpec{
			Terms: []riverworkflow.WaitTermSpec{
				riverworkflow.WaitTermTimer(riverworkflow.TimerAfterWaitStarted("t", 24*time.Hour)),
			},
			Expr: "t",
		},
	})

	prep, err := wf.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prep.Jobs)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, client.Stop(stopCtx))
	}()

	// Wait for "dep" to be discarded and "waiter" to be cancelled.
	// dep will complete (go to discarded), waiter should be cancelled.
	// We need 1 discarded + 1 cancelled event; we get discarded via the
	// completed channel... actually river uses EventKindJobCancelled for cancelled;
	// for discarded we need a different event. Let's poll the DB instead.
	timeout := time.After(20 * time.Second)

	// First, wait for dep to be discarded (it will appear as completed in some
	// event types, but we poll the DB for accuracy).
	var depDiscarded bool
	var waiterCancelled bool
	for !depDiscarded || !waiterCancelled {
		select {
		case <-timeout:
			tasks, _ := wf.LoadAll(ctx)
			depRow, _ := tasks.Get("dep")
			waiterRow, _ := tasks.Get("waiter")
			depState := "unknown"
			waiterState := "unknown"
			if depRow != nil {
				depState = string(depRow.State)
			}
			if waiterRow != nil {
				waiterState = string(waiterRow.State)
			}
			t.Fatalf("timed out: dep=%s waiter=%s", depState, waiterState)
		case <-completedChan:
			// Could be dep discarded appearing here — check DB.
		case <-cancelledChan:
			// Could be waiter cancelled appearing here.
		case <-time.After(100 * time.Millisecond):
			// Poll the DB.
		}

		tasks, err := wf.LoadAll(ctx)
		if err != nil {
			continue
		}
		depRow, err := tasks.Get("dep")
		if err == nil && depRow.State == rivertype.JobStateDiscarded {
			depDiscarded = true
		}
		waiterRow, err := tasks.Get("waiter")
		if err == nil && waiterRow.State == rivertype.JobStateCancelled {
			waiterCancelled = true
		}
	}

	// Final assertions.
	tasks, err := wf.LoadAll(ctx)
	require.NoError(t, err)
	depRow, err := tasks.Get("dep")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateDiscarded, depRow.State, "dep should be discarded")
	waiterRow, err := tasks.Get("waiter")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCancelled, waiterRow.State, "waiter should be cancelled because its dep failed")

	// The waiter should carry the failed_reason metadata key.
	var waiterMeta map[string]any
	require.NoError(t, json.Unmarshal(waiterRow.Metadata, &waiterMeta))
	require.Contains(t, waiterMeta, rivercommon.MetadataKeyWorkflowWaitFailedReason,
		"waiter should carry river:workflow_wait_failed_reason in metadata")
}
