package river

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

// TestDurablePeriodicJobs_HydratesPersistedNextRunAt verifies the core promise
// of durable periodic jobs: when the client (re)starts, the enqueuer reads any
// previously-persisted next run time from river_periodic_job and uses it,
// instead of recomputing from ScheduleFunc(now). The test pre-seeds a row,
// boots a client whose periodic job has the same ID, waits for hydration, and
// confirms the persisted row is unchanged (no premature insert, no overwrite).
func TestDurablePeriodicJobs_HydratesPersistedNextRunAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	// Pre-seed a periodic job row directly via the executor. NextRunAt is far
	// in the future so the enqueuer won't fire it during the test.
	persisted := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	exec := driver.GetExecutor()
	_, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
		Schema: schema,
		Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
			{ID: "hydration_test_job", NextRunAt: persisted, UpdatedAt: time.Now().UTC()},
		},
	})
	require.NoError(t, err)

	// Start a client whose periodic job ID matches the seeded row. The 1 ms
	// interval would normally cause the enqueuer to fire on the next tick;
	// hydration should override that with the persisted future NextRunAt.
	config := newTestConfig(t, schema)
	config.DurablePeriodicJobs = DurablePeriodicJobsConfig{Enabled: true}
	config.PeriodicJobs = []*PeriodicJob{
		NewPeriodicJob(
			PeriodicInterval(time.Millisecond),
			func() (JobArgs, *InsertOpts) { return noOpArgs{}, nil },
			&PeriodicJobOpts{ID: "hydration_test_job"},
		),
	}

	client := newTestClient(t, dbPool, config)
	client.testSignals.Init(t)
	startClient(ctx, t, client)

	// Wait for the enqueuer to enter its main loop, which is when hydration
	// has happened and any RunOnStart inserts would have fired.
	client.testSignals.periodicJobEnqueuer.EnteredLoop.WaitOrTimeout()

	// Verify the persisted row is still there with its original NextRunAt.
	// If hydration didn't work, the enqueuer would have recomputed and
	// upserted a fresh NextRunAt close to "now," not 2h out.
	rows, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{Schema: schema})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "hydration_test_job", rows[0].ID)
	require.WithinDuration(t, persisted, rows[0].NextRunAt, time.Second,
		"NextRunAt must remain the persisted value; the enqueuer must not recompute from now")

	// Also verify that no job got inserted (NextRunAt is 2h out and the job
	// has no RunOnStart).
	count, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
		Schema: schema,
		State:  "available",
	})
	require.NoError(t, err)
	require.Zero(t, count, "no job should be inserted while persisted NextRunAt is in the future")
}

// TestDurablePeriodicJobs_RunOnStartFiresRegardlessOfPersisted verifies the
// Pro-documented RunOnStart semantics under durable scheduling: even when a
// future NextRunAt is persisted, a RunOnStart job fires on every leader start.
func TestDurablePeriodicJobs_RunOnStartFiresRegardlessOfPersisted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	exec := driver.GetExecutor()
	persisted := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	_, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
		Schema: schema,
		Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
			{ID: "run_on_start_job", NextRunAt: persisted, UpdatedAt: time.Now().UTC()},
		},
	})
	require.NoError(t, err)

	config := newTestConfig(t, schema)
	config.DurablePeriodicJobs = DurablePeriodicJobsConfig{Enabled: true}
	config.PeriodicJobs = []*PeriodicJob{
		NewPeriodicJob(
			PeriodicInterval(time.Hour),
			func() (JobArgs, *InsertOpts) { return noOpArgs{}, nil },
			&PeriodicJobOpts{ID: "run_on_start_job", RunOnStart: true},
		),
	}

	client := newTestClient(t, dbPool, config)
	client.testSignals.Init(t)
	startClient(ctx, t, client)

	client.testSignals.periodicJobEnqueuer.EnteredLoop.WaitOrTimeout()
	client.testSignals.periodicJobEnqueuer.InsertedJobs.WaitOrTimeout()

	count, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
		Schema: schema,
		State:  "available",
	})
	require.NoError(t, err)
	require.Equal(t, 1, count, "RunOnStart must fire even when a future NextRunAt is persisted")
}
