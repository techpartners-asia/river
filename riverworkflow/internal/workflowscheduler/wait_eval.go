package workflowscheduler

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/river/riverworkflow/internal/waiteval"
)

// waitSpecJSON is a private struct that mirrors the JSON shape of a WaitSpec as
// stored in the river:workflow_wait metadata key. It must not import the parent
// riverworkflow package (that would create an import cycle since riverworkflow
// imports the scheduler via client.go). Instead we replicate the json tags here.
type waitSpecJSON struct {
	Terms []waitTermSpecJSON `json:"terms"`
	Expr  string             `json:"expr"`
}

type waitTermSpecJSON struct {
	Name    string         `json:"name"`
	Kind    string         `json:"kind"`
	Key     string         `json:"key,omitempty"`
	CELExpr string         `json:"cel_expr,omitempty"`
	Timer   *timerSpecJSON `json:"timer,omitempty"`
	Label   string         `json:"label,omitempty"`
}

type timerSpecJSON struct {
	Name        string        `json:"name"`
	Kind        string        `json:"kind"`
	At          time.Time     `json:"at,omitzero"`
	Dur         time.Duration `json:"dur,omitempty"`
	DepTaskName string        `json:"dep_task_name,omitempty"`
}

// programCacheEntry holds a compiled CEL program keyed by the SHA-256 hash of
// the raw WaitSpec JSON. The scheduler is single-goroutine so no mutex is
// needed.
type programCacheEntry struct {
	prog *waiteval.Program
}

// evaluateWaits is the scheduler's wait-resolution pass. It:
//  1. Lists all pending tasks that carry a river:workflow_wait key.
//  2. For each, classifies its deps.
//  3. If deps Failed → cancel the task.
//  4. If deps Pending → skip.
//  5. If deps Satisfied → record wait_started_at on first sight; compile + eval
//     the CEL wait condition. If true → promote.
func (s *WorkflowScheduler) evaluateWaits(ctx context.Context, progCache map[[32]byte]*programCacheEntry) error {
	now := s.Time.Now().UTC()

	// List pending wait-bearing tasks using the dialect-correct driver method.
	// Previously this used JobList with a raw `metadata ? 'key'` Postgres-only
	// jsonb operator, which caused SQLite to treat `?` as a positional bind
	// placeholder and error every tick, hanging all wait-bearing tasks.
	iterCtx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	rows, err := s.exec.JobGetWorkflowWaitTasks(iterCtx, &riverdriver.JobGetWorkflowWaitTasksParams{
		Max:    s.config.BatchSize,
		Schema: s.config.Schema,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("evaluateWaits: list pending wait tasks: %w", err)
	}

	if len(rows) == 0 {
		return nil
	}

	s.Logger.DebugContext(ctx, s.Name+": evaluateWaits: found pending wait tasks",
		slog.Int("count", len(rows)))

	// Cache workflow siblings within this tick: workflowID → sibling-name map.
	siblingsCache := map[string]map[string]*rivertype.JobRow{}

	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := s.processWaitTask(ctx, row, now, progCache, siblingsCache); err != nil {
			// Log per-task errors and continue — don't abort the whole pass.
			s.Logger.WarnContext(ctx, s.Name+": evaluateWaits: error processing task",
				slog.Int64("job_id", row.ID),
				slog.String("error", err.Error()))
		}
	}
	return nil
}

// processWaitTask handles a single pending wait task.
func (s *WorkflowScheduler) processWaitTask(
	ctx context.Context,
	row *rivertype.JobRow,
	now time.Time,
	progCache map[[32]byte]*programCacheEntry,
	siblingsCache map[string]map[string]*rivertype.JobRow,
) error {
	// Parse the metadata once.
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(row.Metadata, &meta); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}

	// Extract workflow_id.
	var workflowID string
	if raw, ok := meta[rivercommon.MetadataKeyWorkflowID]; ok {
		_ = json.Unmarshal(raw, &workflowID)
	}
	if workflowID == "" {
		return fmt.Errorf("task has no workflow_id")
	}

	// Extract declared deps.
	var deps []string
	if raw, ok := meta[rivercommon.MetadataKeyWorkflowDeps]; ok {
		_ = json.Unmarshal(raw, &deps)
	}

	// Extract ignore flags (already baked per-task at prepare time).
	ignoreCancelled := boolMetaFlag(meta, rivercommon.MetadataKeyWorkflowIgnoreCancelledDeps)
	ignoreDiscarded := boolMetaFlag(meta, rivercommon.MetadataKeyWorkflowIgnoreDiscardedDeps)
	ignoreDeleted := boolMetaFlag(meta, rivercommon.MetadataKeyWorkflowIgnoreDeletedDeps)

	// Load siblings for this workflow, reusing cache within the tick.
	siblings, err := s.loadSiblings(ctx, workflowID, siblingsCache)
	if err != nil {
		return fmt.Errorf("load siblings for workflow %s: %w", workflowID, err)
	}

	// Determine the current task's own name so we can skip it in dep lookups.
	// We do NOT mutate the cached sibling map — multiple wait tasks in the same
	// workflow share the same cache entry and a delete would corrupt it for the
	// next task. Self is never in its own declared deps, so classifyDeps and
	// the CEL inputs naturally ignore the self entry.
	status := classifyDeps(deps, siblings, ignoreCancelled, ignoreDiscarded, ignoreDeleted)

	switch status {
	case DepStatusFailed:
		iterCtx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
		_, err := s.exec.JobApplyWorkflowWait(iterCtx, &riverdriver.JobApplyWorkflowWaitParams{
			ID:      row.ID,
			Now:     now,
			Outcome: "cancel",
			Schema:  s.config.Schema,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("cancel wait task %d: %w", row.ID, err)
		}
		s.Logger.DebugContext(ctx, s.Name+": evaluateWaits: cancelled wait task (dep failed)",
			slog.Int64("job_id", row.ID))
		return nil

	case DepStatusPending:
		// Not ready yet.
		return nil

	case DepStatusSatisfied:
		// Fall through to wait-expression evaluation below.
	}

	// Deps are satisfied. Ensure wait_started_at is recorded.
	waitStartedAt, err := s.ensureWaitStartedAt(ctx, row, meta, now)
	if err != nil {
		return fmt.Errorf("ensure wait_started_at for task %d: %w", row.ID, err)
	}

	// Parse and compile the WaitSpec.
	rawSpec, ok := meta[rivercommon.MetadataKeyWorkflowWait]
	if !ok {
		return fmt.Errorf("task %d has no wait spec in metadata", row.ID)
	}

	var spec waitSpecJSON
	if err := json.Unmarshal(rawSpec, &spec); err != nil {
		return fmt.Errorf("parse wait spec for task %d: %w", row.ID, err)
	}

	// Look up or compile the CEL program.
	cacheKey := sha256.Sum256(rawSpec)
	entry, found := progCache[cacheKey]
	if !found {
		terms := toEngineTerms(spec.Terms)
		prog, err := waiteval.Compile(terms, spec.Expr)
		if err != nil {
			return fmt.Errorf("compile wait spec for task %d: %w", row.ID, err)
		}
		entry = &programCacheEntry{prog: prog}
		progCache[cacheKey] = entry
	}

	// Build timer anchors.
	anchors := waiteval.TimerAnchors{
		// Use the task's own CreatedAt as the workflow creation anchor.
		// Tasks in a workflow insert together so CreatedAt ≈ workflow creation time.
		WorkflowCreatedAt: row.CreatedAt,
		WaitStartedAt:     waitStartedAt,
		DepFinalizedAt:    make(map[string]time.Time),
	}
	for name, sib := range siblings {
		if sib.FinalizedAt != nil {
			anchors.DepFinalizedAt[name] = *sib.FinalizedAt
		}
	}

	// Build Inputs: resolve timers and build deps view.
	inputs := waiteval.Inputs{
		Timers:  make(map[string]bool),
		Signals: map[string]any{}, // CP3: signals not implemented yet
		Deps:    make(map[string]waiteval.DepView),
	}

	for _, term := range spec.Terms {
		if term.Kind == "timer" && term.Timer != nil {
			timerSpec := waiteval.TimerSpecData{
				Name:        term.Timer.Name,
				Kind:        term.Timer.Kind,
				At:          term.Timer.At,
				Dur:         term.Timer.Dur,
				DepTaskName: term.Timer.DepTaskName,
			}
			fired, _, err := waiteval.ResolveTimer(timerSpec, anchors, now)
			if err != nil {
				return fmt.Errorf("resolve timer %q for task %d: %w", term.Name, row.ID, err)
			}
			inputs.Timers[term.Name] = fired
		}
	}

	// Build dep views from sibling outputs.
	for name, sib := range siblings {
		var sibMeta map[string]json.RawMessage
		var output any
		if err := json.Unmarshal(sib.Metadata, &sibMeta); err == nil {
			if rawOut, ok := sibMeta[rivertype.MetadataKeyOutput]; ok {
				_ = json.Unmarshal(rawOut, &output)
			}
		}
		inputs.Deps[name] = waiteval.DepView{
			Output: output,
			State:  string(sib.State),
		}
	}

	// Evaluate the wait expression.
	ready, err := entry.prog.Evaluate(inputs)
	if err != nil {
		return fmt.Errorf("evaluate wait expr for task %d: %w", row.ID, err)
	}

	if !ready {
		return nil
	}

	// Promote the task.
	iterCtx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	_, err = s.exec.JobApplyWorkflowWait(iterCtx, &riverdriver.JobApplyWorkflowWaitParams{
		ID:      row.ID,
		Now:     now,
		Outcome: "promote",
		Schema:  s.config.Schema,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("promote wait task %d: %w", row.ID, err)
	}
	s.Logger.DebugContext(ctx, s.Name+": evaluateWaits: promoted wait task",
		slog.Int64("job_id", row.ID))
	return nil
}

// ensureWaitStartedAt reads river:workflow_wait_started_at from metadata or
// writes it (via JobUpdate with merge) on first sight. Returns the anchor time.
func (s *WorkflowScheduler) ensureWaitStartedAt(
	ctx context.Context,
	row *rivertype.JobRow,
	meta map[string]json.RawMessage,
	now time.Time,
) (time.Time, error) {
	if raw, ok := meta[rivercommon.MetadataKeyWorkflowWaitStartedAt]; ok {
		var ts string
		if err := json.Unmarshal(raw, &ts); err == nil && ts != "" {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				return t, nil
			}
		}
	}

	// Not yet recorded — write it now.
	update := map[string]string{
		rivercommon.MetadataKeyWorkflowWaitStartedAt: now.UTC().Format(time.RFC3339Nano),
	}
	updateBytes, err := json.Marshal(update)
	if err != nil {
		return time.Time{}, fmt.Errorf("marshal wait_started_at: %w", err)
	}

	iterCtx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	_, err = s.exec.JobUpdate(iterCtx, &riverdriver.JobUpdateParams{
		ID:              row.ID,
		MetadataDoMerge: true,
		Metadata:        updateBytes,
		Schema:          s.config.Schema,
	})
	cancel()
	if err != nil {
		return time.Time{}, fmt.Errorf("write wait_started_at: %w", err)
	}

	return now, nil
}

// loadSiblings fetches all sibling tasks for a workflow, with per-tick caching
// to avoid N calls for N wait tasks in the same workflow.
func (s *WorkflowScheduler) loadSiblings(
	ctx context.Context,
	workflowID string,
	cache map[string]map[string]*rivertype.JobRow,
) (map[string]*rivertype.JobRow, error) {
	if cached, ok := cache[workflowID]; ok {
		return cached, nil
	}

	iterCtx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	rows, err := s.exec.JobGetWorkflowTasks(iterCtx, &riverdriver.JobGetWorkflowTasksParams{
		Schema:     s.config.Schema,
		WorkflowID: workflowID,
	})
	cancel()
	if err != nil {
		return nil, err
	}

	m := make(map[string]*rivertype.JobRow, len(rows))
	for _, r := range rows {
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(r.Metadata, &meta); err != nil {
			continue
		}
		var taskName string
		if raw, ok := meta[rivercommon.MetadataKeyWorkflowTask]; ok {
			_ = json.Unmarshal(raw, &taskName)
		}
		if taskName != "" {
			m[taskName] = r
		}
	}

	cache[workflowID] = m
	return m, nil
}

// boolMetaFlag reads a JSON bool from metadata, returning false if absent or
// not a bool.
func boolMetaFlag(meta map[string]json.RawMessage, key string) bool {
	raw, ok := meta[key]
	if !ok {
		return false
	}
	var b bool
	_ = json.Unmarshal(raw, &b)
	return b
}

// toEngineTerms maps our private waitTermSpecJSON to waiteval.TermData.
func toEngineTerms(terms []waitTermSpecJSON) []waiteval.TermData {
	out := make([]waiteval.TermData, len(terms))
	for i, t := range terms {
		out[i] = waiteval.TermData{
			Name:     t.Name,
			Kind:     t.Kind,
			Key:      t.Key,
			CELExpr:  t.CELExpr,
			HasTimer: t.Timer != nil,
		}
	}
	return out
}
