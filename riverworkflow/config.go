package riverworkflow

import (
	"time"

	"github.com/riverqueue/river"
)

// Config configures a [Client]. It embeds [river.Config] and adds tunables
// specific to workflows.
type Config struct {
	river.Config

	// WorkflowScheduler tunes the leader-elected service that promotes
	// pending workflow tasks to their next state once their dependencies
	// complete. Zero values use defaults.
	WorkflowScheduler WorkflowSchedulerConfig
}

// WorkflowSchedulerConfig tunes the WorkflowScheduler service.
type WorkflowSchedulerConfig struct {
	// BatchSize caps how many pending workflow tasks the scheduler will scan
	// per tick. Defaults to 1000.
	BatchSize int

	// Interval is the tick interval for the scheduler. Defaults to 5s.
	Interval time.Duration

	// SignalScanLimit caps the number of signals loaded per workflow per tick
	// when evaluating signal-gated wait tasks. Defaults to 10000.
	SignalScanLimit int

	// WorkflowTimerPollerInterval is the tick interval used when the scheduler
	// needs to re-evaluate timer-based wait expressions. When > 0, the
	// scheduler's effective tick interval is min(Interval, WorkflowTimerPollerInterval).
	// Defaults to 1s.
	WorkflowTimerPollerInterval time.Duration
}

func (c *Config) applyDefaults() {
	if c.WorkflowScheduler.Interval <= 0 {
		c.WorkflowScheduler.Interval = 5 * time.Second
	}
	if c.WorkflowScheduler.BatchSize <= 0 {
		c.WorkflowScheduler.BatchSize = 1000
	}
	if c.WorkflowScheduler.SignalScanLimit <= 0 {
		c.WorkflowScheduler.SignalScanLimit = 10_000
	}
	if c.WorkflowScheduler.WorkflowTimerPollerInterval <= 0 {
		c.WorkflowScheduler.WorkflowTimerPollerInterval = 1 * time.Second
	}
}
