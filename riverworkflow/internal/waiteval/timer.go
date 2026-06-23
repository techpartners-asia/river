package waiteval

import (
	"fmt"
	"time"
)

// TimerSpecData holds the specification for a timer anchor resolution.
type TimerSpecData struct {
	Name        string        // Timer name (for reference)
	Kind        string        // Timer kind: "at", "after_wait_started", "after_workflow_created", "after_task_finalized"
	At          time.Time     // Absolute fire time (for "at" kind)
	Dur         time.Duration // Duration to add to anchor (for other kinds)
	DepTaskName string        // Task name dependency (for "after_task_finalized" kind)
}

// TimerAnchors holds the reference timestamps for timer resolution.
type TimerAnchors struct {
	WorkflowCreatedAt time.Time
	WaitStartedAt     time.Time
	DepFinalizedAt    map[string]time.Time // keyed by task name
}

// ResolveTimer resolves a timer specification against anchors and current time.
// It returns:
// - fired: whether the timer has fired (now >= fireAt)
// - fireAt: the absolute instant the timer fires
// - err: an error if the kind is unknown or other issues occur
//
// For "after_task_finalized" with a missing/zero dependency, returns fired=false,
// fireAt=time.Time{}, err=nil (the dependency hasn't finalized yet).
func ResolveTimer(spec TimerSpecData, anchors TimerAnchors, now time.Time) (fired bool, fireAt time.Time, err error) {
	switch spec.Kind {
	case "at":
		fireAt = spec.At
		fired = now.After(fireAt) || now.Equal(fireAt)
		return fired, fireAt, nil

	case "after_wait_started":
		fireAt = anchors.WaitStartedAt.Add(spec.Dur)
		fired = now.After(fireAt) || now.Equal(fireAt)
		return fired, fireAt, nil

	case "after_workflow_created":
		fireAt = anchors.WorkflowCreatedAt.Add(spec.Dur)
		fired = now.After(fireAt) || now.Equal(fireAt)
		return fired, fireAt, nil

	case "after_task_finalized":
		depTime, present := anchors.DepFinalizedAt[spec.DepTaskName]
		// If dependency hasn't finalized (missing or zero), timer cannot fire yet
		if !present || depTime.IsZero() {
			return false, time.Time{}, nil
		}
		fireAt = depTime.Add(spec.Dur)
		fired = now.After(fireAt) || now.Equal(fireAt)
		return fired, fireAt, nil

	default:
		return false, time.Time{}, fmt.Errorf("unknown timer kind %q", spec.Kind)
	}
}
