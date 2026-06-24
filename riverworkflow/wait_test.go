package riverworkflow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		require.NoError(t, s.Validate())
	})

	t.Run("EmptyExpr", func(t *testing.T) {
		s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("t", "true")}, Expr: ""}
		require.ErrorIs(t, s.Validate(), ErrWaitExprEmpty)
	})

	t.Run("EmptyTermName", func(t *testing.T) {
		s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("", "true")}, Expr: "x"}
		require.ErrorIs(t, s.Validate(), ErrWaitTermNameEmpty)
	})

	t.Run("DuplicateTermName", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTerm("a", "true"), WaitTerm("a", "false")},
			Expr:  "a",
		}
		require.ErrorIs(t, s.Validate(), ErrWaitTermNameDuplicate)
	})

	t.Run("TimerAfterTaskFinalizedNeedsDep", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerAfterTaskFinalized("d", "", time.Minute))},
			Expr:  "d",
		}
		require.ErrorIs(t, s.Validate(), ErrWaitTimerAnchorInvalid)
	})

	t.Run("TimerUnknownKind", func(t *testing.T) {
		// A bad kind would otherwise pass validation and then fail at
		// ResolveTimer every tick, hanging the task forever.
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerSpec{Name: "d", Kind: "after_made_up"})},
			Expr:  "d",
		}
		require.ErrorIs(t, s.Validate(), ErrWaitTimerAnchorInvalid)
	})

	t.Run("TimerEmptyKind", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerSpec{Name: "d"})},
			Expr:  "d",
		}
		require.ErrorIs(t, s.Validate(), ErrWaitTimerAnchorInvalid)
	})

	t.Run("TimerAtNeedsNonZeroTime", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerAt("d", time.Time{}))},
			Expr:  "d",
		}
		require.ErrorIs(t, s.Validate(), ErrWaitTimerAnchorInvalid)
	})

	t.Run("TimerNegativeDuration", func(t *testing.T) {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerAfterWaitStarted("d", -time.Minute))},
			Expr:  "d",
		}
		require.ErrorIs(t, s.Validate(), ErrWaitTimerAnchorInvalid)
	})
}

func TestWaitTermBuilders(t *testing.T) {
	sig := WaitTermSignal("n", "k", "payload.ok").Label("human approval")
	require.Equal(t, WaitTermKindSignal, sig.Kind)
	require.Equal(t, "k", sig.Key)
	require.Equal(t, "human approval", sig.LabelText)

	tim := WaitTermTimer(TimerAt("when", time.Unix(0, 0)))
	require.Equal(t, WaitTermKindTimer, tim.Kind)
	require.NotNil(t, tim.Timer)
	require.Equal(t, TimerKindAt, tim.Timer.Kind)
}

func TestWaitSpecValidateRejectsBadCEL(t *testing.T) {
	s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("a", "1 +")}, Expr: "a"}
	if err := s.Validate(); err == nil {
		t.Fatal("expected CEL syntax error from Validate")
	}
}

func TestWaitSpecValidateRejectsUnknownTermInExpr(t *testing.T) {
	s := &WaitSpec{Terms: []WaitTermSpec{WaitTerm("a", "true")}, Expr: "a && ghost"}
	if err := s.Validate(); err == nil {
		t.Fatal("expected error for undefined term 'ghost' in Expr")
	}
}

func TestWaitSpecValidateRejectsNonBoolExpr(t *testing.T) {
	// A non-bool top-level expr would otherwise pass validation and then hang
	// the task forever at scheduler time (evalBool never sees a bool).
	for _, expr := range []string{"1 + 1", `"x"`, "deadline ? 1 : 0"} {
		s := &WaitSpec{
			Terms: []WaitTermSpec{WaitTermTimer(TimerAfterWaitStarted("deadline", time.Hour))},
			Expr:  expr,
		}
		if err := s.Validate(); err == nil {
			t.Fatalf("expected non-bool top-level expr %q to be rejected", expr)
		}
	}
}

func TestParseWaitSpecRoundTrip(t *testing.T) {
	orig := &WaitSpec{
		Terms: []WaitTermSpec{WaitTermSignal("ok", "approved", "payload.ok")},
		Expr:  "ok",
	}
	raw, err := json.Marshal(orig)
	require.NoError(t, err)
	got, err := parseWaitSpec(raw)
	require.NoError(t, err)
	require.Equal(t, orig.Expr, got.Expr)
	require.Len(t, got.Terms, 1)
	require.Equal(t, "approved", got.Terms[0].Key)
}
