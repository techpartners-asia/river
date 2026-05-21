// Package workflowscheduler periodically promotes pending workflow tasks
// whose dependencies have reached a terminal state.
package workflowscheduler

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
)

// IntervalDefault is the default poll interval.
const IntervalDefault = 5 * time.Second

// BatchSizeDefault is the default maximum number of pending workflow tasks
// the scheduler will process per tick.
const BatchSizeDefault = 1000

// Config configures a [WorkflowScheduler].
type Config struct {
	BatchSize int
	Interval  time.Duration
	Schema    string
}

// TestSignals exposes signals used by tests to wait for scheduler activity.
type TestSignals struct {
	ScheduledBatch testsignal.TestSignal[struct{}]
}

// Init readies the test signals for use by tests.
func (ts *TestSignals) Init(tb testutil.TestingTB) {
	ts.ScheduledBatch.Init(tb)
}

// WorkflowScheduler is a [startstop.Service] that periodically calls the
// driver's JobUpdateWorkflowReady to promote eligible pending workflow tasks.
type WorkflowScheduler struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop
	baseservice.BaseService

	TestSignals TestSignals

	config *Config
	exec   riverdriver.Executor
}

// New constructs a new WorkflowScheduler.
func New(archetype *baseservice.Archetype, config *Config, exec riverdriver.Executor) *WorkflowScheduler {
	return baseservice.Init(archetype, &WorkflowScheduler{
		config: &Config{
			BatchSize: cmp.Or(config.BatchSize, BatchSizeDefault),
			Interval:  cmp.Or(config.Interval, IntervalDefault),
			Schema:    config.Schema,
		},
		exec: exec,
	})
}

// Start begins the service's background polling loop.
func (s *WorkflowScheduler) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	s.StaggerStart(ctx)

	go func() {
		started()
		defer stopped()

		s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStarted)
		defer s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStopped)

		ticker := timeutil.NewTickerWithInitialTick(ctx, s.config.Interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			if err := s.runOnce(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					s.Logger.ErrorContext(ctx, s.Name+": Error promoting workflow tasks", slog.String("error", err.Error()))
				}
			}
			s.TestSignals.ScheduledBatch.Signal(struct{}{})
		}
	}()

	return nil
}

func (s *WorkflowScheduler) runOnce(ctx context.Context) error {
	// Deadline enforcement runs once per tick, before promotion. Promotion
	// only moves pending → ready, so a deadline that fires during promotion
	// still gets caught on the next tick; running it first means tasks past
	// their deadline don't waste a promotion-eligibility check.
	if err := s.cancelExpiredWorkflows(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			s.Logger.ErrorContext(ctx, s.Name+": Error cancelling expired workflows", slog.String("error", err.Error()))
		}
	}

	for {
		// H3: Check outer context before each iteration so total runOnce time is
		// bounded even when every individual per-iteration timeout is 30 s.
		if err := ctx.Err(); err != nil {
			return err
		}

		iterCtx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
		rows, err := s.exec.JobUpdateWorkflowReady(iterCtx, &riverdriver.JobUpdateWorkflowReadyParams{
			Max:    s.config.BatchSize,
			Now:    s.Time.Now(),
			Schema: s.config.Schema,
		})
		cancel()
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			s.Logger.DebugContext(ctx, s.Name+": Promoted workflow tasks", slog.Int("num_promoted", len(rows)))
		}
		if len(rows) < s.config.BatchSize {
			return nil
		}

		// H1: Full batch returned — more rows likely remain. Sleep briefly before
		// the next iteration to avoid hot-spinning and to give other services a
		// chance to run, matching the pattern used by job_scheduler and
		// job_rescuer in the core maintenance package.
		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}
}

// cancelExpiredWorkflows finds every workflow with at least one non-terminal
// task whose recorded deadline has passed, then cancels the entire workflow
// via the standard JobCancelWorkflow path. Running tasks are not finalized
// directly — they receive a control-topic notification so their executor
// cancels through the worker's context, matching how user-initiated workflow
// cancellation behaves.
func (s *WorkflowScheduler) cancelExpiredWorkflows(ctx context.Context) error {
	now := s.Time.Now().UTC()

	// Inline now into the WHERE clause via a quoted timestamp literal rather
	// than NamedArgs so this query stays portable across the drivers' JobList
	// implementations (each handles parameter substitution slightly
	// differently and not all support NamedArgs uniformly).
	nowLiteral := fmt.Sprintf("'%s'::timestamptz", now.Format(time.RFC3339Nano))
	whereClause := `state IN ('available','pending','retryable','running','scheduled')
		AND metadata ? 'river:workflow_deadline_at'
		AND (metadata->>'river:workflow_deadline_at')::timestamptz < ` + nowLiteral

	iterCtx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	rows, err := s.exec.JobList(iterCtx, &riverdriver.JobListParams{
		Max:           int32(s.config.BatchSize),
		OrderByClause: "id",
		Schema:        s.config.Schema,
		WhereClause:   whereClause,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("listing expired workflow tasks: %w", err)
	}
	if len(rows) > 0 {
		s.Logger.DebugContext(ctx, s.Name+": Expired workflow task scan",
			slog.Int("rows_found", len(rows)))
	}

	// Dedup workflow_ids: any row in the expired set is enough to know its
	// whole workflow needs cancellation, and JobCancelWorkflow takes one
	// workflow_id at a time.
	seen := map[string]bool{}
	for _, row := range rows {
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(row.Metadata, &meta); err != nil {
			continue
		}
		var wfID string
		if raw, ok := meta[rivercommon.MetadataKeyWorkflowID]; ok {
			_ = json.Unmarshal(raw, &wfID)
		}
		if wfID == "" || seen[wfID] {
			continue
		}
		seen[wfID] = true

		iterCancelCtx, cancelCancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
		cancelled, err := s.exec.JobCancelWorkflow(iterCancelCtx, &riverdriver.JobCancelWorkflowParams{
			CancelAttemptedAt: s.Time.Now(),
			ControlTopic:      string(notifier.NotificationTopicControl),
			Now:               s.Time.Now(),
			Reason:            "workflow deadline exceeded",
			Schema:            s.config.Schema,
			WorkflowID:        wfID,
		})
		cancelCancel()
		if err != nil {
			s.Logger.WarnContext(ctx, s.Name+": Failed to cancel expired workflow",
				slog.String("workflow_id", wfID),
				slog.String("error", err.Error()))
			continue
		}
		s.Logger.InfoContext(ctx, s.Name+": Cancelled expired workflow",
			slog.String("workflow_id", wfID),
			slog.Int("tasks_cancelled", len(cancelled)))
	}
	return nil
}
