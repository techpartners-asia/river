package rivercommon

import (
	"errors"
	"regexp"
)

// These constants are made available in rivercommon so that they're accessible
// by internal packages, but the top-level river package re-exports them, and
// all user code must use that set instead.
const (
	// AllQueuesString is a special string that can be used to indicate all
	// queues in some operations, particularly pause and resume.
	AllQueuesString    = "*"
	MaxAttemptsDefault = 25
	PriorityDefault    = 1
	QueueDefault       = "default"
)

const (
	// MetadataKeyPeriodicJobID is a metadata key inserted with a periodic job
	// when a configured periodic job has its ID property set. This lets
	// inserted jobs easily be traced back to the periodic job that created
	// them.
	MetadataKeyPeriodicJobID = "river:periodic_job_id"

	// MetadataKeyResumableStep records the last successfully completed step for
	// a resumable job so later attempts can skip ahead.
	MetadataKeyResumableStep = "river:resumable_step"

	// MetadataKeyResumableCursor records a resumable step cursor so a later
	// attempt can resume a partially completed step.
	MetadataKeyResumableCursor = "river:resumable_cursor"

	// MetadataKeyRescueCount records how many times the job has been rescued.
	MetadataKeyRescueCount = "river:rescue_count"

	// MetadataKeyUniqueNonce is a special metadata key used by the SQLite driver to
	// determine whether an upsert is was skipped or not because the `(xmax != 0)`
	// trick we use in Postgres doesn't work in SQLite.
	MetadataKeyUniqueNonce = "river:unique_nonce"

	// MetadataKeyWorkflowDeadlineAt records the workflow's deadline as an
	// RFC3339 timestamp. Tasks of the workflow whose state is still
	// non-terminal past this point are cancelled by the workflow scheduler
	// with reason "workflow deadline exceeded".
	MetadataKeyWorkflowDeadlineAt = "river:workflow_deadline_at"

	// MetadataKeyWorkflowDeps holds a JSON array of task names that a
	// workflow task depends on.
	MetadataKeyWorkflowDeps = "river:workflow_deps"

	// MetadataKeyWorkflowWait holds the JSON-serialized WaitSpec for a task.
	// A task carrying this key is held pending by the dep-promotion SQL and
	// promoted only by the workflow scheduler once its wait resolves.
	MetadataKeyWorkflowWait = "river:workflow_wait"

	// MetadataKeyWorkflowID identifies the workflow a task belongs to.
	MetadataKeyWorkflowID = "river:workflow_id"

	// MetadataKeyWorkflowIgnoreCancelledDeps, when set to true, causes a
	// cancelled dep to be treated as a successful dep for promotion.
	MetadataKeyWorkflowIgnoreCancelledDeps = "river:workflow_ignore_cancelled_deps"

	// MetadataKeyWorkflowIgnoreDeletedDeps mirrors the above for deleted deps.
	MetadataKeyWorkflowIgnoreDeletedDeps = "river:workflow_ignore_deleted_deps"

	// MetadataKeyWorkflowIgnoreDiscardedDeps mirrors the above for discarded deps.
	MetadataKeyWorkflowIgnoreDiscardedDeps = "river:workflow_ignore_discarded_deps"

	// MetadataKeyWorkflowName is an optional human-readable workflow label.
	MetadataKeyWorkflowName = "river:workflow_name"

	// MetadataKeyWorkflowTask is the unique-within-workflow task name.
	MetadataKeyWorkflowTask = "river:workflow_task"

	// MetadataKeyWorkflowWaitResolvedAt records the RFC3339 timestamp at which
	// a workflow wait condition was resolved (i.e. the task was promoted).
	MetadataKeyWorkflowWaitResolvedAt = "river:workflow_wait_resolved_at"

	// MetadataKeyWorkflowWaitFailedReason records the reason a workflow wait
	// task was cancelled (e.g. "dependency failed").
	MetadataKeyWorkflowWaitFailedReason = "river:workflow_wait_failed_reason"
)

type ContextKeyClient struct{}

// ErrStop is a special error injected by the client into its fetch and work
// CancelCauseFuncs when it's stopping. It may be used by components for such
// cases like avoiding logging an error during a normal shutdown procedure.
var ErrStop = errors.New("stop initiated")

// UserSpecifiedIDOrKindRE is a regular expression to which the format of job
// kinds and some other user-specified IDs (e.g. periodic job names) must
// comply. Mainly, minimal special characters, and excluding spaces and commas
// which are problematic for the search UI.
var UserSpecifiedIDOrKindRE = regexp.MustCompile(`\A[\w][\w\-\[\]<>\/.·:+]+\z`)
