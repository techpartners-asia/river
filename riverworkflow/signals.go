package riverworkflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// ErrSignalPayloadMismatch is re-exported from rivertype for callers who import
// only riverworkflow. Use errors.Is to test for this error.
var ErrSignalPayloadMismatch = rivertype.ErrWorkflowSignalPayloadMismatch

// signalScanLimit is the default maximum number of signals loaded by List,
// ListForTask, and LatestForTask when Max is not specified.
const signalScanLimit = 10_000

// WorkflowSignals is a handle for emitting and listing signals on a specific
// workflow. Obtain one via [Workflow.Signals].
type WorkflowSignals[TTx any] struct {
	workflowID string
	exec       riverdriver.Executor
	driver     riverdriver.Driver[TTx]
	schema     string
}

// Signals returns a handle for emitting and listing signals on this workflow.
func (w *Workflow[TTx]) Signals() *WorkflowSignals[TTx] {
	return &WorkflowSignals[TTx]{
		workflowID: w.id,
		exec:       w.exec,
		driver:     w.driver,
		schema:     w.schema,
	}
}

// WorkflowSignalEmitOpts contains optional parameters for [WorkflowSignals.Emit]
// and [WorkflowSignals.EmitTx].
type WorkflowSignalEmitOpts struct {
	// IdempotencyKey is an optional deduplication key scoped to
	// (workflow_id, idempotency_key). A second Emit with the same key and
	// an identical payload is a no-op (returns the original signal). A
	// differing payload returns [ErrSignalPayloadMismatch].
	IdempotencyKey string

	// Source is an optional label indicating the originating system or process.
	Source string
}

// Emit marshals payload as JSON and emits it as a signal with the given key to
// this workflow. If opts is non-nil, opts.IdempotencyKey enables deduplication
// and opts.Source labels the emitting system. See [WorkflowSignalEmitOpts] for
// details.
//
// Returns [ErrSignalPayloadMismatch] (via errors.Is) if opts.IdempotencyKey
// is set and was previously used with a different payload.
func (s *WorkflowSignals[TTx]) Emit(ctx context.Context, key string, payload any, opts *WorkflowSignalEmitOpts) (*rivertype.WorkflowSignal, error) {
	return s.emitOn(ctx, s.exec, key, payload, opts)
}

// EmitTx is the transactional variant of [WorkflowSignals.Emit]. The signal is
// emitted within the given transaction and is only visible once the transaction
// commits.
func (s *WorkflowSignals[TTx]) EmitTx(ctx context.Context, tx TTx, key string, payload any, opts *WorkflowSignalEmitOpts) (*rivertype.WorkflowSignal, error) {
	return s.emitOn(ctx, s.driver.UnwrapExecutor(tx), key, payload, opts)
}

func (s *WorkflowSignals[TTx]) emitOn(ctx context.Context, exec riverdriver.Executor, key string, payload any, opts *WorkflowSignalEmitOpts) (*rivertype.WorkflowSignal, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("riverworkflow: marshal signal payload: %w", err)
	}

	params := &riverdriver.WorkflowSignalEmitParams{
		WorkflowID: s.workflowID,
		SignalKey:  key,
		Payload:    payloadBytes,
		Now:        time.Now(),
		Schema:     s.schema,
	}
	if opts != nil {
		if opts.IdempotencyKey != "" {
			ik := opts.IdempotencyKey
			params.IdempotencyKey = &ik
		}
		if opts.Source != "" {
			src := opts.Source
			params.Source = &src
		}
	}

	sig, err := exec.WorkflowSignalEmit(ctx, params)
	if err != nil {
		// Surface ErrSignalPayloadMismatch unchanged; callers use errors.Is.
		return nil, err
	}
	return sig, nil
}

// WorkflowSignalListParams configures [WorkflowSignals.List].
type WorkflowSignalListParams struct {
	// SignalKey optionally filters to signals with this key. Zero value means
	// all signal keys for this workflow.
	SignalKey string

	// Max is the maximum number of signals to return. Defaults to
	// signalScanLimit (10 000) when zero.
	Max int
}

// List returns signals for this workflow, optionally filtered by key.
// Results are ordered by (created_at, id) ascending (historical/audit view).
// Uses OrderByNewest:false so results are not truncated at the newest end.
//
// List always includes resolved signals (resolved_at IS NOT NULL) because it
// is a full historical/audit view. Use [WorkflowSignals.ListForTask] with
// IncludeAfterResolution:false (the default) to restrict to unresolved signals
// only.
func (s *WorkflowSignals[TTx]) List(ctx context.Context, params *WorkflowSignalListParams) ([]*rivertype.WorkflowSignal, error) {
	p := &riverdriver.WorkflowSignalListParams{
		WorkflowID:      s.workflowID,
		IncludeResolved: true, // audit view: always include resolved signals
		Max:             signalScanLimit,
		Schema:          s.schema,
	}
	if params != nil {
		if params.SignalKey != "" {
			k := params.SignalKey
			p.SignalKey = &k
		}
		if params.Max > 0 {
			p.Max = params.Max
		}
	}
	return s.exec.WorkflowSignalList(ctx, p)
}

// WorkflowSignalListForTaskParams configures [WorkflowSignals.ListForTask].
type WorkflowSignalListForTaskParams struct {
	// IncludeAfterResolution, when true, includes signals whose resolved_at is
	// set. When false (the default), only unresolved signals are returned.
	//
	// Note: resolved_at is reserved for future resolution-marking. No signals
	// are currently marked resolved by this library, so this field has no
	// effect until a resolution writer is wired (CP4+).
	IncludeAfterResolution bool

	// Max is the maximum number of signals to return. Defaults to
	// signalScanLimit (10 000) when zero.
	Max int
}

// ListForTask returns signals for this workflow filtered by signal key. The
// taskName parameter is accepted for API parity with the Pro edition but is
// not used for filtering in CP3; per-task resolution views are a CP4 feature.
// Results are ordered by (created_at, id) ascending (historical/audit view).
// Uses OrderByNewest:false so results are not truncated at the newest end.
func (s *WorkflowSignals[TTx]) ListForTask(ctx context.Context, taskName, key string, params *WorkflowSignalListForTaskParams) ([]*rivertype.WorkflowSignal, error) {
	// taskName is accepted for future per-task filtering (CP4). In CP3 we
	// filter by workflow+key only.
	_ = taskName

	p := &riverdriver.WorkflowSignalListParams{
		WorkflowID: s.workflowID,
		Max:        signalScanLimit,
		Schema:     s.schema,
	}
	if key != "" {
		p.SignalKey = &key
	}
	if params != nil {
		p.IncludeResolved = params.IncludeAfterResolution
		if params.Max > 0 {
			p.Max = params.Max
		}
	}
	return s.exec.WorkflowSignalList(ctx, p)
}

// WorkflowSignalLatestForTaskOpts configures [WorkflowSignals.LatestForTask].
type WorkflowSignalLatestForTaskOpts struct {
	// IncludeAfterResolution, when true, includes signals whose resolved_at is
	// set. When false (the default), only unresolved signals are considered.
	//
	// Note: resolved_at is reserved for future resolution-marking. No signals
	// are currently marked resolved by this library, so this field has no
	// effect until a resolution writer is wired (CP4+).
	IncludeAfterResolution bool
}

// LatestForTask returns the most recently created signal for the given key,
// or nil if no signal has been emitted. The taskName parameter is accepted
// for API parity with the Pro edition but is not used for filtering in CP3;
// per-task resolution views are a CP4 feature.
//
// "Latest" is determined by (created_at DESC, id DESC). Uses
// OrderByNewest:true so the first returned row is the newest, avoiding
// truncation of recent signals when there are more than signalScanLimit rows.
func (s *WorkflowSignals[TTx]) LatestForTask(ctx context.Context, taskName, key string, opts *WorkflowSignalLatestForTaskOpts) (*rivertype.WorkflowSignal, error) {
	// taskName is accepted for future per-task filtering (CP4). In CP3 we
	// filter by workflow+key only.
	_ = taskName

	p := &riverdriver.WorkflowSignalListParams{
		WorkflowID:    s.workflowID,
		Max:           signalScanLimit,
		OrderByNewest: true, // DESC so first row = newest; safe with truncation
		Schema:        s.schema,
	}
	if key != "" {
		p.SignalKey = &key
	}
	if opts != nil {
		p.IncludeResolved = opts.IncludeAfterResolution
	}
	rows, err := s.exec.WorkflowSignalList(ctx, p)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	// With OrderByNewest:true, rows are ordered (created_at DESC, id DESC):
	// the first element is the newest signal.
	return rows[0], nil
}
