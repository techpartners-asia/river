// Tiny helper to seed a sample workflow into a local Postgres for the
// riverui demo. Run from repo root:
//
//	go run ./cmd/seed-workflow
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverworkflow"
)

type stepArgs struct {
	Step string `json:"step"`
}

func (stepArgs) Kind() string { return "demo_step" }

type stepWorker struct {
	river.WorkerDefaults[stepArgs]
}

func (*stepWorker) Work(_ context.Context, job *river.Job[stepArgs]) error {
	dur := 4 * time.Second
	if d := os.Getenv("STEP_DURATION"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			dur = parsed
		}
	}
	time.Sleep(dur)
	slog.Info("worked workflow step", slog.String("step", job.Args.Step))
	return nil
}

func main() {
	ctx := context.Background()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://localhost/river_test"
	}
	dbPool, err := pgxpool.New(ctx, url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &stepWorker{})

	client, err := riverworkflow.NewClient(riverpgxv5.New(dbPool), &riverworkflow.Config{
		Config: river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
			Workers: workers,
		},
		WorkflowScheduler: riverworkflow.WorkflowSchedulerConfig{
			Interval: 500 * time.Millisecond,
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	wf := client.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "demo billing pipeline"})
	a := wf.Add("ingest", stepArgs{Step: "ingest"}, nil, nil)
	b1 := wf.Add("charge", stepArgs{Step: "charge"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{a.Name}})
	b2 := wf.Add("receipt", stepArgs{Step: "receipt"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{a.Name}})
	wf.Add("notify", stepArgs{Step: "notify"}, nil, &riverworkflow.WorkflowTaskOpts{Deps: []string{b1.Name, b2.Name}})

	prep, err := wf.Prepare(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var _ pgx.Tx
	if _, err := client.InsertMany(ctx, prep.Jobs); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := client.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("seeded workflow %s with %d tasks\n", prep.WorkflowID, len(prep.Jobs))
	fmt.Printf("URL: http://localhost:8080/workflows/%s\n", prep.WorkflowID)
	fmt.Println("client running; press Ctrl+C to stop")

	<-ctx.Done()
}
