// Package workflowscheduler periodically promotes pending workflow tasks
// whose dependencies have reached a terminal state.
package workflowscheduler

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
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
	for {
		ctx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
		rows, err := s.exec.JobUpdateWorkflowReady(ctx, &riverdriver.JobUpdateWorkflowReadyParams{
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
	}
}
