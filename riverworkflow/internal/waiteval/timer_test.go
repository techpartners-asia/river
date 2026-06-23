package waiteval

import (
	"testing"
	"time"
)

func TestResolveTimerAt(t *testing.T) {
	base := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	fired, fireAt, err := ResolveTimer(TimerSpecData{Kind: "at", At: base}, TimerAnchors{}, base.Add(time.Second))
	if err != nil || !fired || !fireAt.Equal(base) {
		t.Fatalf("want fired,%v; got %v,%v,%v", base, fired, fireAt, err)
	}
	fired, _, _ = ResolveTimer(TimerSpecData{Kind: "at", At: base}, TimerAnchors{}, base.Add(-time.Second))
	if fired {
		t.Fatal("must not fire before At")
	}
}

func TestResolveTimerAfterWaitStarted(t *testing.T) {
	ws := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	spec := TimerSpecData{Kind: "after_wait_started", Dur: time.Hour}
	fired, fireAt, _ := ResolveTimer(spec, TimerAnchors{WaitStartedAt: ws}, ws.Add(90*time.Minute))
	if !fired || !fireAt.Equal(ws.Add(time.Hour)) {
		t.Fatalf("want fired at +1h; got %v,%v", fired, fireAt)
	}
	fired, _, _ = ResolveTimer(spec, TimerAnchors{WaitStartedAt: ws}, ws.Add(30*time.Minute))
	if fired {
		t.Fatal("must not fire before +1h")
	}
}

func TestResolveTimerAfterTaskFinalizedMissing(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	fired, _, err := ResolveTimer(TimerSpecData{Kind: "after_task_finalized", DepTaskName: "a", Dur: time.Minute}, TimerAnchors{DepFinalizedAt: map[string]time.Time{}}, now)
	if err != nil || fired {
		t.Fatalf("unfinalized dep must not fire and not error; got %v,%v", fired, err)
	}
}
