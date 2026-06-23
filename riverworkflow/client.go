package riverworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/river/riverworkflow/internal/workflowscheduler"
)

// Client wraps [*river.Client] with workflow-construction and
// workflow-cancellation methods. All non-workflow methods on the embedded
// client pass through unchanged.
type Client[TTx any] struct {
	*river.Client[TTx]

	driver riverdriver.Driver[TTx]
	config *Config
}

// NewClient constructs a riverworkflow [Client] backed by the given driver.
// It also installs a leader-elected workflow scheduler service into the
// underlying [*river.Client] via the driver-plugin mechanism.
func NewClient[TTx any](driver riverdriver.Driver[TTx], config *Config) (*Client[TTx], error) {
	if driver == nil {
		return nil, errors.New("riverworkflow: driver is required")
	}
	if config == nil {
		config = &Config{}
	}
	config.applyDefaults()

	pilot := &workflowPilot{
		exec: driver.GetExecutor(),
		schedCfg: &workflowscheduler.Config{
			BatchSize:           config.WorkflowScheduler.BatchSize,
			Interval:            config.WorkflowScheduler.Interval,
			Schema:              config.Schema,
			SignalScanLimit:     config.WorkflowScheduler.SignalScanLimit,
			TimerPollerInterval: config.WorkflowScheduler.WorkflowTimerPollerInterval,
		},
	}
	plugin := &workflowDriverPlugin[TTx]{Driver: driver, pilot: pilot}

	riverClient, err := river.NewClient(plugin, &config.Config)
	if err != nil {
		return nil, err
	}

	return &Client[TTx]{
		Client: riverClient,
		driver: driver,
		config: config,
	}, nil
}

// NewWorkflow starts a new workflow build. Call [Workflow.Add] to append tasks
// and [Workflow.Prepare] to validate and render insertion parameters.
func (c *Client[TTx]) NewWorkflow(opts *WorkflowOpts) *Workflow[TTx] {
	return newWorkflow[TTx](opts, c.driver, c.config.Schema)
}

// WorkflowFromExisting constructs a Workflow handle for an existing workflow,
// using the workflow ID stored in the given job row's metadata. New tasks may
// be added via [Workflow.Add] and inserted via [Workflow.Prepare] +
// [river.Client.InsertMany].
func (c *Client[TTx]) WorkflowFromExisting(jobRow *rivertype.JobRow, opts *WorkflowOpts) (*Workflow[TTx], error) {
	if jobRow == nil {
		return nil, fmt.Errorf("riverworkflow: jobRow is nil")
	}
	id, err := workflowIDFromMetadata(jobRow.Metadata)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &WorkflowOpts{}
	}
	opts.ID = id
	return newWorkflow[TTx](opts, c.driver, c.config.Schema), nil
}

// WorkflowCancelResult is returned by [Client.WorkflowCancel] and
// [Client.WorkflowCancelTx].
type WorkflowCancelResult struct {
	CancelledJobs []*rivertype.JobRow
}

// WorkflowCancel cancels every non-finalized task in the workflow identified
// by workflowID. Completed, cancelled, and discarded tasks are left alone.
func (c *Client[TTx]) WorkflowCancel(ctx context.Context, workflowID string) (*WorkflowCancelResult, error) {
	return c.cancelOn(ctx, c.driver.GetExecutor(), workflowID)
}

// WorkflowCancelTx is the transactional variant of [Client.WorkflowCancel].
func (c *Client[TTx]) WorkflowCancelTx(ctx context.Context, tx TTx, workflowID string) (*WorkflowCancelResult, error) {
	return c.cancelOn(ctx, c.driver.UnwrapExecutor(tx), workflowID)
}

func (c *Client[TTx]) cancelOn(ctx context.Context, exec riverdriver.Executor, workflowID string) (*WorkflowCancelResult, error) {
	now := time.Now()
	rows, err := exec.JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
		CancelAttemptedAt: now,
		ControlTopic:      string(notifier.NotificationTopicControl),
		Now:               now,
		Reason:            "workflow cancelled by client",
		Schema:            c.config.Schema,
		WorkflowID:        workflowID,
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowCancelResult{CancelledJobs: rows}, nil
}

// WorkflowRetryResult is returned by [Client.WorkflowRetry] and
// [Client.WorkflowRetryTx].
type WorkflowRetryResult struct {
	RetriedJobs []*rivertype.JobRow
}

// WorkflowRetry retries workflow tasks according to the given mode:
//   - "failed_only": resets only discarded tasks
//   - "failed_and_downstream": resets discarded and cancelled tasks
//   - "all": resets cancelled, completed, and discarded tasks
//
// If resetHistory is true, the error history on each reset task is cleared.
func (c *Client[TTx]) WorkflowRetry(ctx context.Context, workflowID, mode string, resetHistory bool) (*WorkflowRetryResult, error) {
	return c.retryOn(ctx, c.driver.GetExecutor(), workflowID, mode, resetHistory)
}

// WorkflowRetryTx is the transactional variant of [Client.WorkflowRetry].
func (c *Client[TTx]) WorkflowRetryTx(ctx context.Context, tx TTx, workflowID, mode string, resetHistory bool) (*WorkflowRetryResult, error) {
	return c.retryOn(ctx, c.driver.UnwrapExecutor(tx), workflowID, mode, resetHistory)
}

func (c *Client[TTx]) retryOn(ctx context.Context, exec riverdriver.Executor, workflowID, mode string, resetHistory bool) (*WorkflowRetryResult, error) {
	rows, err := exec.JobRetryWorkflow(ctx, &riverdriver.JobRetryWorkflowParams{
		Mode:         mode,
		Now:          time.Now(),
		ResetHistory: resetHistory,
		Schema:       c.config.Schema,
		WorkflowID:   workflowID,
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowRetryResult{RetriedJobs: rows}, nil
}

func workflowIDFromMetadata(metadata []byte) (string, error) {
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &meta); err != nil {
		return "", fmt.Errorf("riverworkflow: parse metadata: %w", err)
	}
	raw, ok := meta[rivercommon.MetadataKeyWorkflowID]
	if !ok {
		return "", fmt.Errorf("riverworkflow: job has no workflow metadata")
	}
	var id string
	if err := json.Unmarshal(raw, &id); err != nil {
		return "", fmt.Errorf("riverworkflow: parse workflow id: %w", err)
	}
	if id == "" {
		return "", fmt.Errorf("riverworkflow: workflow id in job metadata is empty")
	}
	return id, nil
}
