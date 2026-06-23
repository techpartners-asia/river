package riverworkflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river/riverworkflow/internal/waiteval"
)

// Wait term kinds.
const (
	WaitTermKindSignal  = "signal"
	WaitTermKindTimer   = "timer"
	WaitTermKindGeneric = "generic"
)

// Timer anchor kinds.
const (
	TimerKindAt                   = "at"
	TimerKindAfterWaitStarted     = "after_wait_started"
	TimerKindAfterWorkflowCreated = "after_workflow_created"
	TimerKindAfterTaskFinalized   = "after_task_finalized"
)

var (
	ErrWaitExprEmpty          = errors.New("riverworkflow: wait spec Expr is empty")
	ErrWaitTermNameEmpty      = errors.New("riverworkflow: wait term name is empty")
	ErrWaitTermNameDuplicate  = errors.New("riverworkflow: duplicate wait term name")
	ErrWaitTimerAnchorInvalid = errors.New("riverworkflow: invalid timer anchor")
)

// TimerSpec describes a time anchor for a timer wait term. Construct via the
// Timer* builders rather than directly.
type TimerSpec struct {
	Name        string        `json:"name"`
	Kind        string        `json:"kind"`
	At          time.Time     `json:"at,omitzero"`             // TimerKindAt
	Dur         time.Duration `json:"dur,omitempty"`           // relative kinds
	DepTaskName string        `json:"dep_task_name,omitempty"` // TimerKindAfterTaskFinalized
}

func TimerAt(name string, t time.Time) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAt, At: t}
}
func TimerAfterWaitStarted(name string, d time.Duration) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAfterWaitStarted, Dur: d}
}
func TimerAfterWorkflowCreated(name string, d time.Duration) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAfterWorkflowCreated, Dur: d}
}
func TimerAfterTaskFinalized(name, depTaskName string, d time.Duration) TimerSpec {
	return TimerSpec{Name: name, Kind: TimerKindAfterTaskFinalized, DepTaskName: depTaskName, Dur: d}
}

// WaitTermSpec is a single named predicate within a WaitSpec.
type WaitTermSpec struct {
	Name      string     `json:"name"`
	Kind      string     `json:"kind"`
	Key       string     `json:"key,omitempty"`      // signal key (signal terms)
	CELExpr   string     `json:"cel_expr,omitempty"` // signal-scoped or generic CEL
	Timer     *TimerSpec `json:"timer,omitempty"`    // timer terms
	LabelText string     `json:"label,omitempty"`
}

// Label sets a human-readable label and returns the term for chaining.
func (t WaitTermSpec) Label(s string) WaitTermSpec { t.LabelText = s; return t }

func WaitTermSignal(name, key, celExpr string) WaitTermSpec {
	return WaitTermSpec{Name: name, Kind: WaitTermKindSignal, Key: key, CELExpr: celExpr}
}
func WaitTermTimer(spec TimerSpec) WaitTermSpec {
	return WaitTermSpec{Name: spec.Name, Kind: WaitTermKindTimer, Timer: &spec}
}
func WaitTerm(name, celExpr string) WaitTermSpec {
	return WaitTermSpec{Name: name, Kind: WaitTermKindGeneric, CELExpr: celExpr}
}

// WaitSpec is a CEL boolean expression over named terms; a wait-bearing task
// is promoted only when Expr evaluates true.
type WaitSpec struct {
	Terms []WaitTermSpec `json:"terms"`
	Expr  string         `json:"expr"`
}

// Validate performs structural validation and CEL syntax validation of Expr
// and term CELExpr fields.
func (s *WaitSpec) Validate() error {
	if s.Expr == "" {
		return ErrWaitExprEmpty
	}
	seen := make(map[string]struct{}, len(s.Terms))
	for _, t := range s.Terms {
		if t.Name == "" {
			return ErrWaitTermNameEmpty
		}
		if _, dup := seen[t.Name]; dup {
			return fmt.Errorf("%w: %q", ErrWaitTermNameDuplicate, t.Name)
		}
		seen[t.Name] = struct{}{}
		if t.Kind == WaitTermKindTimer {
			if t.Timer == nil {
				return fmt.Errorf("%w: term %q has nil timer", ErrWaitTimerAnchorInvalid, t.Name)
			}
			if t.Timer.Kind == TimerKindAfterTaskFinalized && t.Timer.DepTaskName == "" {
				return fmt.Errorf("%w: term %q requires a dep task name", ErrWaitTimerAnchorInvalid, t.Name)
			}
		}
	}
	if _, err := waiteval.Compile(s.toEngineTerms(), s.Expr); err != nil {
		return err
	}
	return nil
}

// toEngineTerms maps each WaitTermSpec to a waiteval.TermData for CEL
// compilation and evaluation.
func (s *WaitSpec) toEngineTerms() []waiteval.TermData {
	terms := make([]waiteval.TermData, len(s.Terms))
	for i, t := range s.Terms {
		terms[i] = waiteval.TermData{
			Name:     t.Name,
			Kind:     t.Kind,
			Key:      t.Key,
			CELExpr:  t.CELExpr,
			HasTimer: t.Timer != nil,
		}
	}
	return terms
}

// parseWaitSpec unmarshals the metadata JSON back into a *WaitSpec,
// round-tripping the json tags defined on WaitSpec and WaitTermSpec.
func parseWaitSpec(raw []byte) (*WaitSpec, error) {
	var s WaitSpec
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("riverworkflow: parse wait spec: %w", err)
	}
	return &s, nil
}
