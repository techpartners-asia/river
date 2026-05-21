package river_test

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

type DurablePeriodicJobArgs struct{}

func (DurablePeriodicJobArgs) Kind() string { return "durable_periodic" }

type DurablePeriodicJobWorker struct {
	river.WorkerDefaults[DurablePeriodicJobArgs]
}

func (w *DurablePeriodicJobWorker) Work(ctx context.Context, job *river.Job[DurablePeriodicJobArgs]) error {
	fmt.Printf("Durable periodic job ran; its next run time is persisted across restarts\n")
	return nil
}

// Example_durablePeriodicJob demonstrates configuring a durable periodic job
// whose next run time is persisted to the river_periodic_job table, so the
// schedule survives client restarts, crashes, and leader elections. The
// PeriodicJobOpts.ID is required for a job to be durable.
func Example_durablePeriodicJob() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &DurablePeriodicJobWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), initTestConfig(ctx, dbPool, &river.Config{
		DurablePeriodicJobs: river.DurablePeriodicJobsConfig{
			Enabled: true,
		},
		PeriodicJobs: []*river.PeriodicJob{
			river.NewPeriodicJob(
				river.PeriodicInterval(15*time.Minute),
				func() (river.JobArgs, *river.InsertOpts) {
					return DurablePeriodicJobArgs{}, nil
				},
				&river.PeriodicJobOpts{
					ID:         "durable_periodic_demo",
					RunOnStart: true,
				},
			),
		},
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	}))
	if err != nil {
		panic(err)
	}

	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Durable periodic job ran; its next run time is persisted across restarts
}
