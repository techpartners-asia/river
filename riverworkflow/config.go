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
}

func (c *Config) applyDefaults() {
	if c.WorkflowScheduler.Interval <= 0 {
		c.WorkflowScheduler.Interval = 5 * time.Second
	}
	if c.WorkflowScheduler.BatchSize <= 0 {
		c.WorkflowScheduler.BatchSize = 1000
	}
}
