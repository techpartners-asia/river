package riverworkflow

import (
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
