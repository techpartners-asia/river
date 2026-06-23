package riverworkflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/river/riverworkflow/internal/waiteval"
	"github.com/riverqueue/river/riverworkflow/internal/workflowid"
)

// WaitPhase describes the current resolution phase of a wait-bearing task.
type WaitPhase string

const (
	// WaitPhaseNoWait means the task has no wait spec.
	WaitPhaseNoWait WaitPhase = "no_wait"

	// WaitPhasePending means the wait spec has not yet resolved (ExprResult == false).
	WaitPhasePending WaitPhase = "pending"

	// WaitPhaseResolved means the wait expression evaluated to true.
	WaitPhaseResolved WaitPhase = "resolved"
)

// WaitTermDiagnostic is the per-term evaluation snapshot returned by
// [Workflow.WaitDiagnostics].
type WaitTermDiagnostic struct {
	// Name is the term name as declared in the WaitSpec.
	Name string

	// Kind is the term kind ("signal", "timer", "generic").
	Kind string

	// Label is the human-readable label set via WaitTermSpec.Label(), or empty.
	Label string

	// Result is the evaluated boolean result for this term.
	Result bool

	// Detail is a short human-readable note from the evaluator.
	Detail string
}

// WaitDiagnostics is a read-only snapshot of a task's wait expression
// evaluation. Returned by [Workflow.WaitDiagnostics] and
// [Workflow.WaitDiagnosticsTx].
//
// Note: [WaitPhaseResolved] means the wait expression evaluated to true, not
// that the task will be promoted. The workflow scheduler also requires all
// declared dependencies to be satisfied before promoting a task; this snapshot
// only reflects the wait-expression layer.
type WaitDiagnostics struct {
	// Phase is the high-level resolution state.
	Phase WaitPhase

	// Summary is a one-line human roll-up of the evaluation result.
	Summary string

	// ExprResult is the top-level boolean result of the wait expression.
	ExprResult bool

	// Truncated is true when the signal scan was limited (more signals exist
	// than the scan limit), so the evaluation may be stale.
	Truncated bool

	// Terms holds the per-term evaluation results.
	Terms []WaitTermDiagnostic
}

// WorkflowWaitDiagnosticsOpts configures [Workflow.WaitDiagnostics].
type WorkflowWaitDiagnosticsOpts struct {
	// SignalScanLimit is the maximum number of signals to load from the DB.
	// Defaults to 10,000. Capped at 100,000.
	SignalScanLimit int

	// IncludeAfterResolution, when true, returns diagnostics even when the task
	// has already been promoted (Task 3 feature; wired here for API parity).
	IncludeAfterResolution bool
}

const (
	waitDiagSignalScanLimitDefault = 10_000
	waitDiagSignalScanLimitMax     = 100_000
)

// WaitDiagnostics returns a read-only snapshot of the wait expression
// evaluation for the named task in this workflow. It mirrors the semantics of
// the workflow scheduler's input-building so diagnostics agree with what the
// scheduler would compute. Signal-load errors are returned directly to the
// caller (a read API); unlike the scheduler, which logs and continues.
func (w *Workflow[TTx]) WaitDiagnostics(ctx context.Context, taskName string, opts *WorkflowWaitDiagnosticsOpts) (*WaitDiagnostics, error) {
	return w.waitDiagnosticsOnExec(ctx, w.exec, taskName, opts)
}

// WaitDiagnosticsTx is the transactional variant of [Workflow.WaitDiagnostics].
func (w *Workflow[TTx]) WaitDiagnosticsTx(ctx context.Context, tx TTx, taskName string, opts *WorkflowWaitDiagnosticsOpts) (*WaitDiagnostics, error) {
	return w.waitDiagnosticsOnExec(ctx, w.driver.UnwrapExecutor(tx), taskName, opts)
}

func (w *Workflow[TTx]) waitDiagnosticsOnExec(ctx context.Context, exec riverdriver.Executor, taskName string, opts *WorkflowWaitDiagnosticsOpts) (*WaitDiagnostics, error) {
	if exec == nil {
		return nil, fmt.Errorf("riverworkflow: workflow has no driver bound")
	}

	// Resolve scan limit.
	scanLimit := waitDiagSignalScanLimitDefault
	if opts != nil && opts.SignalScanLimit > 0 {
		scanLimit = opts.SignalScanLimit
	}
	if scanLimit > waitDiagSignalScanLimitMax {
		scanLimit = waitDiagSignalScanLimitMax
	}

	// Load all workflow tasks (reuses the same path as LoadAll/WorkflowTasks).
	rows, err := exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
		Schema:     w.schema,
		WorkflowID: w.id,
	})
	if err != nil {
		return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: load tasks: %w", err)
	}

	// Build a name-keyed sibling map (same logic as workflowscheduler.loadSiblings).
	siblings := make(map[string]*rivertype.JobRow, len(rows))
	for _, r := range rows {
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(r.Metadata, &meta); err != nil {
			continue
		}
		raw, ok := meta[rivercommon.MetadataKeyWorkflowTask]
		if !ok {
			continue
		}
		var name string
		if err := json.Unmarshal(raw, &name); err != nil || name == "" {
			continue
		}
		siblings[name] = r
	}

	// Find the target task.
	targetRow, ok := siblings[taskName]
	if !ok {
		return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: task %q not found in workflow %s", taskName, w.id)
	}

	// Parse the target task's metadata.
	var targetMeta map[string]json.RawMessage
	if err := json.Unmarshal(targetRow.Metadata, &targetMeta); err != nil {
		return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: parse metadata for task %q: %w", taskName, err)
	}

	// If no wait spec, return the no-wait phase immediately.
	rawSpec, hasWait := targetMeta[rivercommon.MetadataKeyWorkflowWait]
	if !hasWait {
		return &WaitDiagnostics{Phase: WaitPhaseNoWait}, nil
	}

	// Parse the wait spec.
	spec, err := parseWaitSpec(rawSpec)
	if err != nil {
		return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: parse wait spec for task %q: %w", taskName, err)
	}

	// Build label map from spec terms (EvalReport.Terms has no label).
	labelByName := make(map[string]string, len(spec.Terms))
	for _, t := range spec.Terms {
		labelByName[t.Name] = t.LabelText
	}

	// Parse declared dep names.
	var depNames []string
	if raw, ok := targetMeta[rivercommon.MetadataKeyWorkflowDeps]; ok {
		_ = json.Unmarshal(raw, &depNames)
	}

	// --- Build waiteval.Inputs mirroring the scheduler's wait_eval.go ---

	now := time.Now().UTC()

	// 1. Timer anchors.
	//    WorkflowCreatedAt: decode from ULID timestamp; fall back to task CreatedAt.
	workflowCreatedAt, err := workflowid.Timestamp(w.id)
	if err != nil {
		workflowCreatedAt = targetRow.CreatedAt
	}

	//    WaitStartedAt: from river:workflow_wait_started_at metadata (zero if absent).
	//    When zero, after_wait_started timers fire against zero-time which would
	//    incorrectly be in the past. We guard this explicitly: a zero anchor means
	//    the scheduler hasn't written wait_started_at yet, so after_wait_started
	//    timers are not yet fired (honest snapshot).
	var waitStartedAt time.Time
	if raw, ok := targetMeta[rivercommon.MetadataKeyWorkflowWaitStartedAt]; ok {
		var ts string
		if err := json.Unmarshal(raw, &ts); err == nil && ts != "" {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				waitStartedAt = t
			}
		}
	}

	//    DepFinalizedAt: from sibling FinalizedAt fields.
	anchors := waiteval.TimerAnchors{
		WorkflowCreatedAt: workflowCreatedAt,
		WaitStartedAt:     waitStartedAt,
		DepFinalizedAt:    make(map[string]time.Time),
	}
	for name, sib := range siblings {
		if sib.FinalizedAt != nil {
			anchors.DepFinalizedAt[name] = *sib.FinalizedAt
		}
	}

	// 2. Timers: resolve each timer term.
	timers := make(map[string]bool)
	for _, term := range spec.Terms {
		if term.Kind == WaitTermKindTimer && term.Timer != nil {
			// Guard: if timer requires wait_started_at anchor and it's absent,
			// treat as not fired (honest snapshot — the scheduler hasn't set
			// the anchor yet, so the timer cannot meaningfully fire).
			if term.Timer.Kind == TimerKindAfterWaitStarted && waitStartedAt.IsZero() {
				timers[term.Name] = false
				continue
			}
			timerSpec := waiteval.TimerSpecData{
				Name:        term.Timer.Name,
				Kind:        term.Timer.Kind,
				At:          term.Timer.At,
				Dur:         term.Timer.Dur,
				DepTaskName: term.Timer.DepTaskName,
			}
			fired, _, err := waiteval.ResolveTimer(timerSpec, anchors, now)
			if err != nil {
				return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: resolve timer %q: %w", term.Name, err)
			}
			timers[term.Name] = fired
		}
	}

	// 3. Signals: load with OrderByNewest:true (mirrors scheduler).
	//    Build latest-per-key SignalView with count as Attempt.
	var signalViews map[string]waiteval.SignalView
	var truncated bool

	// Only load signals if there is at least one signal term.
	hasSignalTerm := false
	for _, term := range spec.Terms {
		if term.Kind == WaitTermKindSignal {
			hasSignalTerm = true
			break
		}
	}

	includeResolved := opts != nil && opts.IncludeAfterResolution

	if hasSignalTerm {
		rawSignals, err := exec.WorkflowSignalList(ctx, &riverdriver.WorkflowSignalListParams{
			WorkflowID:      w.id,
			IncludeResolved: includeResolved,
			Max:             scanLimit,
			OrderByNewest:   true, // DESC so newest signals are not truncated
			Schema:          w.schema,
		})
		if err != nil {
			return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: load signals: %w", err)
		}

		truncated = len(rawSignals) >= scanLimit

		// Build latest-per-key (max by (created_at, id)).
		type keyState struct {
			latest *rivertype.WorkflowSignal
			count  int
		}
		keyMap := make(map[string]*keyState)
		for _, sig := range rawSignals {
			ks, ok := keyMap[sig.SignalKey]
			if !ok {
				ks = &keyState{}
				keyMap[sig.SignalKey] = ks
			}
			ks.count++
			if ks.latest == nil ||
				sig.CreatedAt.After(ks.latest.CreatedAt) ||
				(sig.CreatedAt.Equal(ks.latest.CreatedAt) && sig.ID > ks.latest.ID) {
				ks.latest = sig
			}
		}

		signalViews = make(map[string]waiteval.SignalView, len(keyMap))
		for key, ks := range keyMap {
			var payloadAny any
			if len(ks.latest.Payload) > 0 {
				_ = json.Unmarshal(ks.latest.Payload, &payloadAny)
			}
			var source string
			if ks.latest.Source != nil {
				source = *ks.latest.Source
			}
			signalViews[key] = waiteval.SignalView{
				Payload:   payloadAny,
				Attempt:   ks.count,
				CreatedAt: ks.latest.CreatedAt,
				ID:        ks.latest.ID,
				Source:    source,
			}
		}
	}

	// 4. Deps: each declared dep → DepView with output parsed from metadata.
	depsMap := make(map[string]waiteval.DepView, len(depNames))
	for _, depName := range depNames {
		if sib, ok := siblings[depName]; ok {
			var sibMeta map[string]json.RawMessage
			var output any
			if err := json.Unmarshal(sib.Metadata, &sibMeta); err == nil {
				if rawOut, ok := sibMeta[rivertype.MetadataKeyOutput]; ok {
					_ = json.Unmarshal(rawOut, &output)
				}
			}
			depsMap[depName] = waiteval.DepView{
				Output: output,
				State:  string(sib.State),
			}
		} else {
			// Sibling absent — provide empty placeholder so CEL map-key access
			// does not raise "no such key".
			depsMap[depName] = waiteval.DepView{Output: nil, State: ""}
		}
	}

	inputs := waiteval.Inputs{
		Timers:  timers,
		Signals: signalViews,
		Deps:    depsMap,
	}

	// Compile and evaluate.
	prog, err := waiteval.Compile(spec.toEngineTerms(), spec.Expr)
	if err != nil {
		return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: compile wait spec for task %q: %w", taskName, err)
	}

	report, err := prog.EvaluateReport(inputs)
	if err != nil {
		return nil, fmt.Errorf("riverworkflow: WaitDiagnostics: evaluate wait spec for task %q: %w", taskName, err)
	}

	// Map report terms to WaitTermDiagnostic, adding label from spec.
	termDiags := make([]WaitTermDiagnostic, len(report.Terms))
	for i, tr := range report.Terms {
		termDiags[i] = WaitTermDiagnostic{
			Name:   tr.Name,
			Kind:   tr.Kind,
			Label:  labelByName[tr.Name],
			Result: tr.Result,
			Detail: tr.Detail,
		}
	}

	// Build summary.
	satisfied := 0
	for _, tr := range report.Terms {
		if tr.Result {
			satisfied++
		}
	}
	summary := fmt.Sprintf("%d/%d terms satisfied; expr=%v", satisfied, len(report.Terms), report.ExprResult)

	phase := WaitPhasePending
	if report.ExprResult {
		phase = WaitPhaseResolved
	}

	return &WaitDiagnostics{
		Phase:      phase,
		Summary:    summary,
		ExprResult: report.ExprResult,
		Truncated:  truncated,
		Terms:      termDiags,
	}, nil
}
