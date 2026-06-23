package waiteval

import "testing"

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
	got, err := p.Evaluate(Inputs{Signals: map[string]any{}})
	if err != nil || got {
		t.Fatalf("absent signal must be false; got %v,%v", got, err)
	}
	got, err = p.Evaluate(Inputs{Signals: map[string]any{"approved": map[string]any{"ok": true}}})
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
	got, _ := p.Evaluate(Inputs{Timers: map[string]bool{"t": true}, Signals: map[string]any{}})
	if !got {
		t.Fatal("t||s with t fired must be true")
	}
}
