package riverworkflow

import (
	"errors"
	"testing"
	"time"
)

func TestWaitSpecValidate(t *testing.T) {
	t.Run("ValidSignalAndTimer", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{
				WaitTermSignal("approved", "approved", "payload.ok"),
				WaitTermTimer(TimerAfterWaitStarted("deadline", time.Hour)),
			},
			Expr: "approved || deadline",
		}
		if err := s.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("EmptyExpr", func(t *testing.T) {
		s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("t", "true")}, Expr: ""}
		if !errors.Is(s.Validate(), ErrWaitExprEmpty) {
			t.Fatalf("want ErrWaitExprEmpty, got %v", s.Validate())
		}
	})

	t.Run("EmptyTermName", func(t *testing.T) {
		s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("", "true")}, Expr: "x"}
		if !errors.Is(s.Validate(), ErrWaitTermNameEmpty) {
			t.Fatalf("want ErrWaitTermNameEmpty, got %v", s.Validate())
		}
	})

	t.Run("DuplicateTermName", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTerm("a", "true"), WaitTerm("a", "false")},
			Expr:  "a",
		}
		if !errors.Is(s.Validate(), ErrWaitTermNameDuplicate) {
			t.Fatalf("want ErrWaitTermNameDuplicate, got %v", s.Validate())
		}
	})

	t.Run("TimerAfterTaskFinalizedNeedsDep", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerAfterTaskFinalized("d", "", time.Minute))},
			Expr:  "d",
		}
		if !errors.Is(s.Validate(), ErrWaitTimerAnchorInvalid) {
			t.Fatalf("want ErrWaitTimerAnchorInvalid, got %v", s.Validate())
		}
	})
}

func TestWaitTermBuilders(t *testing.T) {
	sig := WaitTermSignal("n", "k", "payload.ok").Label("human approval")
	if sig.Kind != WaitTermKindSignal || sig.Key != "k" || sig.LabelText != "human approval" {
		t.Fatalf("signal term wrong: %+v", sig)
	}
	tim := WaitTermTimer(TimerAt("when", time.Unix(0, 0)))
	if tim.Kind != WaitTermKindTimer || tim.Timer == nil || tim.Timer.Kind != TimerKindAt {
		t.Fatalf("timer term wrong: %+v", tim)
	}
}
