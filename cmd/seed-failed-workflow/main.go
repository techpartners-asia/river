// Seeds a workflow into Postgres with one discarded task + cascade-cancelled
// downstream tasks. Used for demoing the retry button in riverui.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func main() {
	ctx := context.Background()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://localhost/river_test"
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	exec := riverpgxv5.New(pool).GetExecutor()

	workflowID := fmt.Sprintf("failed-demo-%d", time.Now().Unix())

	insertTask := func(taskName string, state rivertype.JobState, deps []string) {
		metadata := map[string]any{
			rivercommon.MetadataKeyWorkflowID:   workflowID,
			rivercommon.MetadataKeyWorkflowName: "failed billing pipeline",
			rivercommon.MetadataKeyWorkflowTask: taskName,
		}
		if len(deps) > 0 {
			metadata[rivercommon.MetadataKeyWorkflowDeps] = deps
		}
		metaBytes, _ := json.Marshal(metadata)

		var finalizedAt *time.Time
		if state == rivertype.JobStateCancelled || state == rivertype.JobStateCompleted || state == rivertype.JobStateDiscarded {
			ft := time.Now()
			finalizedAt = &ft
		}

		now := time.Now()
		_, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
			EncodedArgs: []byte(`{"step":"` + taskName + `"}`),
			FinalizedAt: finalizedAt,
			Kind:        "demo_step",
			MaxAttempts: 3,
			Metadata:    metaBytes,
			Priority:    1,
			Queue:       "default",
			ScheduledAt: &now,
			State:       state,
			Tags:        []string{},
		})
		if err != nil {
			panic(err)
		}
	}

	// Pipeline: ingest (completed) → charge (discarded, simulating max-attempts exhaustion)
	//                              → receipt (cancelled by cascade)
	// notify depends on charge + receipt and is also cancelled by cascade.
	insertTask("ingest", rivertype.JobStateCompleted, nil)
	insertTask("charge", rivertype.JobStateDiscarded, []string{"ingest"})
	insertTask("receipt", rivertype.JobStateCancelled, []string{"ingest"})
	insertTask("notify", rivertype.JobStateCancelled, []string{"charge", "receipt"})

	fmt.Printf("seeded workflow %s with 1 completed + 1 discarded + 2 cancelled tasks\n", workflowID)
	fmt.Printf("open: http://localhost:8080/workflows/%s\n", workflowID)
	fmt.Println("click the Retry button to reset the 3 failed tasks back to runnable")
}
