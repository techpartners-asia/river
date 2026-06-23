package waiteval

import (
	"testing"
	"time"
)

func TestCompileRejectsBadSyntax(t *testing.T) {
	_, err := Compile([]TermData{{Name: "a", Kind: "generic", CELExpr: "1 +"}}, "a")
	if err == nil {
		t.Fatal("expected compile error for bad CEL syntax")
	}
}

func TestTimerTermFromInputs(t *testing.T) {
	p, err := Compile([]TermData{{Name: "deadline", Kind: "timer", HasTimer: true}}, "deadline")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := p.Evaluate(Inputs{Timers: map[string]bool{"deadline": true}})
	if err != nil || !got {
		t.Fatalf("want true,nil; got %v,%v", got, err)
	}
	got, _ = p.Evaluate(Inputs{Timers: map[string]bool{"deadline": false}})
	if got {
		t.Fatal("want false when timer not fired")
	}
}

func TestSignalTermAbsentIsFalse(t *testing.T) {
	p, err := Compile([]TermData{{Name: "ok", Kind: "signal", Key: "approved", CELExpr: "payload.ok"}}, "ok")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := p.Evaluate(Inputs{Signals: map[string]SignalView{}})
	if err != nil || got {
		t.Fatalf("absent signal must be false; got %v,%v", got, err)
	}
	got, err = p.Evaluate(Inputs{Signals: map[string]SignalView{
		"approved": {Payload: map[string]any{"ok": true}},
	}})
	if err != nil || !got {
		t.Fatalf("present matching signal must be true; got %v,%v", got, err)
	}
}

func TestGenericTermOverDeps(t *testing.T) {
	p, err := Compile([]TermData{{Name: "big", Kind: "generic", CELExpr: `deps["a"].output.n > 5`}}, "big")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := p.Evaluate(Inputs{Deps: map[string]DepView{"a": {Output: map[string]any{"n": 10.0}}}})
	if err != nil || !got {
		t.Fatalf("want true; got %v,%v", got, err)
	}
}

func TestExprCombinesTerms(t *testing.T) {
	p, err := Compile([]TermData{
		{Name: "t", Kind: "timer", HasTimer: true},
		{Name: "s", Kind: "signal", Key: "k", CELExpr: "payload.ok"},
	}, "t || s")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, _ := p.Evaluate(Inputs{Timers: map[string]bool{"t": true}, Signals: map[string]SignalView{}})
	if !got {
		t.Fatal("t||s with t fired must be true")
	}
}

func TestSignalTermEmptyCELExprGatesOnPresence(t *testing.T) {
	p, err := Compile([]TermData{{Name: "got", Kind: "signal", Key: "ping", CELExpr: ""}}, "got")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Absent → false.
	got, err := p.Evaluate(Inputs{Signals: map[string]SignalView{}})
	if err != nil || got {
		t.Fatalf("absent signal must be false; got %v,%v", got, err)
	}
	// Present (any payload, even nil) → true, on presence alone.
	got, err = p.Evaluate(Inputs{Signals: map[string]SignalView{"ping": {Payload: nil}}})
	if err != nil || !got {
		t.Fatalf("present signal with empty CELExpr must be true; got %v,%v", got, err)
	}
}

// TestEvaluateAbsentDepKeyIsFalse verifies that a generic term referencing a
// dep that is not in Inputs.Deps returns (false, nil) — "not yet satisfied" —
// rather than propagating the CEL "no such key" runtime error.
func TestEvaluateAbsentDepKeyIsFalse(t *testing.T) {
	p, err := Compile([]TermData{{Name: "big", Kind: "generic", CELExpr: `deps["x"].output.n > 5`}}, "big")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Evaluate with empty Deps — "x" is absent.
	got, err := p.Evaluate(Inputs{Deps: map[string]DepView{}})
	if err != nil {
		t.Fatalf("absent dep key must return false,nil; got error: %v", err)
	}
	if got {
		t.Fatal("absent dep key must return false, not true")
	}
}

// TestEvaluateAbsentSignalInTopExprIsFalse verifies that a top-level expr
// directly accessing signals["k"] where "k" is absent returns (false, nil).
func TestEvaluateAbsentSignalInTopExprIsFalse(t *testing.T) {
	// A generic term that directly accesses signals["k"].ok in the top-level expr.
	p, err := Compile([]TermData{{Name: "s", Kind: "generic", CELExpr: `signals["k"].ok`}}, "s")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Evaluate with empty Signals — "k" is absent.
	got, err := p.Evaluate(Inputs{Signals: map[string]SignalView{}})
	if err != nil {
		t.Fatalf("absent signal key must return false,nil; got error: %v", err)
	}
	if got {
		t.Fatal("absent signal key must return false, not true")
	}
}

// TestEvaluateScalarPayloadIsFalse verifies that a signal term with a scalar
// payload (e.g. a plain string) evaluated against payload.ok returns (false, nil)
// rather than propagating a CEL field-access-on-scalar runtime error.
func TestEvaluateScalarPayloadIsFalse(t *testing.T) {
	p, err := Compile([]TermData{{Name: "ok", Kind: "signal", Key: "k", CELExpr: "payload.ok"}}, "ok")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Signal "k" is present but its payload is a scalar string, not a map.
	got, err := p.Evaluate(Inputs{Signals: map[string]SignalView{"k": {Payload: "hi"}}})
	if err != nil {
		t.Fatalf("scalar payload must return false,nil; got error: %v", err)
	}
	if got {
		t.Fatal("scalar payload must return false, not true")
	}
}

// TestSignalTermUsesAttemptAndSource verifies that signal sub-expressions can
// reference the full signal metadata: payload fields, attempt count, and source.
func TestSignalTermUsesAttemptAndSource(t *testing.T) {
	p, err := Compile([]TermData{{
		Name:    "ok",
		Kind:    "signal",
		Key:     "approved",
		CELExpr: `payload.ok && attempt > 0 && source == "api"`,
	}}, "ok")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// All conditions met → true.
	got, err := p.Evaluate(Inputs{Signals: map[string]SignalView{
		"approved": {
			Payload: map[string]any{"ok": true},
			Attempt: 1,
			Source:  "api",
		},
	}})
	if err != nil || !got {
		t.Fatalf("all conditions met: want true,nil; got %v,%v", got, err)
	}

	// attempt == 0 → false (attempt > 0 fails).
	got, err = p.Evaluate(Inputs{Signals: map[string]SignalView{
		"approved": {
			Payload: map[string]any{"ok": true},
			Attempt: 0,
			Source:  "api",
		},
	}})
	if err != nil || got {
		t.Fatalf("attempt==0: want false,nil; got %v,%v", got, err)
	}

	// source == "web" → false (source != "api").
	got, err = p.Evaluate(Inputs{Signals: map[string]SignalView{
		"approved": {
			Payload: map[string]any{"ok": true},
			Attempt: 1,
			Source:  "web",
		},
	}})
	if err != nil || got {
		t.Fatalf("source=web: want false,nil; got %v,%v", got, err)
	}
}

// TestSignalTermCreatedAtIsTimestamp verifies that created_at is exposed as a
// CEL timestamp and can be compared with timestamp literals.
func TestSignalTermCreatedAtIsTimestamp(t *testing.T) {
	p, err := Compile([]TermData{{
		Name:    "ok",
		Kind:    "signal",
		Key:     "ping",
		CELExpr: `created_at > timestamp("2020-01-01T00:00:00Z")`,
	}}, "ok")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// CreatedAt is after 2020 → true.
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	got, err := p.Evaluate(Inputs{Signals: map[string]SignalView{
		"ping": {CreatedAt: ts},
	}})
	if err != nil || !got {
		t.Fatalf("created_at after 2020: want true,nil; got %v,%v", got, err)
	}

	// CreatedAt is before 2020 → false.
	tsOld := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err = p.Evaluate(Inputs{Signals: map[string]SignalView{
		"ping": {CreatedAt: tsOld},
	}})
	if err != nil || got {
		t.Fatalf("created_at before 2020: want false,nil; got %v,%v", got, err)
	}
}
