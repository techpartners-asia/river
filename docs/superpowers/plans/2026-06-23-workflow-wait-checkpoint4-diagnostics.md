# Workflow Wait-Family — Checkpoint 4 (Diagnostics) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Checkbox (`- [ ]`) steps.

**Goal:** Add River Pro `Workflow.WaitDiagnostics(ctx, taskName, opts) → WaitDiagnostics{Phase, Summary, ExprResult, Truncated}` — a read-only snapshot of WHY a wait-bearing task is (or isn't) ready, computed by re-evaluating its `WaitSpec` with the same `waiteval` engine the scheduler uses, without promoting, cancelling, or writing evidence. Plus `SignalScanLimit`/`Truncated` surfacing and the `IncludeAfterResolution` option. This completes the wait-family.

**Architecture:** `WaitDiagnostics` lives in the `riverworkflow` package and reuses existing pieces: `parseWaitSpec` (CP2/CP3), the `waiteval` engine, and the driver reads `JobGetWorkflowTasks` (siblings → dep outputs/states) + `WorkflowSignalList` (signals, newest-first). It builds the same `waiteval.Inputs` the scheduler builds (deps + signals + timer anchors), evaluates verbosely (per-term results), and reports. It does NOT reclassify-to-cancel or promote — purely read-only.

**Tech Stack:** Go 1.25, `riverworkflow`, `riverworkflow/internal/waiteval`, `riverdriver`, `rivertype`. (No new migration; `resolved_at` column already exists from CP3.)

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-23-workflow-wait-family-design.md` §10 (diagnostics). CP1–CP3 merged on `master`.
- **API parity (River Pro):** `w.WaitDiagnostics(ctx, taskName string, opts *WorkflowWaitDiagnosticsOpts) (*WaitDiagnostics, error)`; `WaitDiagnostics{Phase WaitPhase; Summary string; ExprResult bool; Truncated bool}`; `WaitPhase` consts incl. `WaitPhasePending`, `WaitPhaseResolved`; `SignalScanLimit` (default 10_000, cap 100_000); `IncludeAfterResolution` option on read APIs.
- **Determinism/read-only:** `WaitDiagnostics` must not mutate any row, must not promote/cancel, must not write `wait_started_at`/`resolved_at` (it reads them). Use `time.Now()` only via an injectable clock if the existing code provides one; otherwise read the current instant once.
- **No scheduler-divergence:** the dep/signal/timer input construction must mirror the scheduler's `processWaitTask` semantics (newest-first signals, latest-per-key, declared-dep population, ULID/metadata/finalized timer anchors). Where logic is duplicated from the scheduler, keep it minimal and add a test asserting diagnostics agrees with real scheduler behavior (pending before, resolved after).
- **All-three-driver lesson (from CP2/CP3 external reviews):** any new read uses the dialect-correct driver methods — NO raw `metadata ? 'key'` SQL in `riverworkflow`.
- Module test root `riverworkflow/`. Postgres test DSN default works; SQLite in-process. Use the `riverdbtest.TestSchema` + pgx harness from `simulation_test.go`/`signals_test.go` for integration tests.

---

### Task 1: `waiteval` — verbose evaluation (per-term results)

**Files:** Modify `riverworkflow/internal/waiteval/waiteval.go`, `waiteval_test.go`.

**Interfaces (produced; consumed by Task 2):**
- `type EvalReport struct { ExprResult bool; Terms []TermReport }`
- `type TermReport struct { Name string; Kind string; Result bool; Detail string }` (`Detail` = short human note, e.g. "signal 'approved' received; payload.ok=true" or "timer fires at 2026-...; not yet" — best-effort).
- `func (p *Program) EvaluateReport(in Inputs) (EvalReport, error)` — evaluates each term (reusing the existing per-term logic) capturing its bool result, then the top-level expr; returns both. The existing `Evaluate` stays (or is reimplemented to call `EvaluateReport` and return `.ExprResult`). Keep the not-ready→false contract (eval errors → false) for both term and expr.

- [ ] **Step 1: Failing tests** in `waiteval_test.go`: `TestEvaluateReportPerTerm` — a spec with a timer term (fired) and a signal term (absent) and `Expr:"t || s"` → `EvalReport{ExprResult:true, Terms:[{Name:"t",Result:true},{Name:"s",Result:false}]}`. Assert per-term results and the overall. Confirm RED.

- [ ] **Step 2: Implement** `EvaluateReport`; refactor `Evaluate` to delegate (`r, err := p.EvaluateReport(in); return r.ExprResult, err`). Preserve all existing behavior (absent-signal→false, empty-CELExpr→presence, eval-error→false). `Detail` can start minimal (term kind + result) — don't overbuild.

- [ ] **Step 3: GREEN** — `cd riverworkflow && go test ./internal/waiteval/`; all prior tests still pass. gofmt/vet clean.

- [ ] **Step 4: Commit** `git commit -m "Add per-term verbose evaluation to waiteval (EvaluateReport)"`

---

### Task 2: `Workflow.WaitDiagnostics` public API

**Files:** Create `riverworkflow/diagnostics.go`, `riverworkflow/diagnostics_test.go`.

**Interfaces (Pro-parity):**
- `type WaitPhase string`; consts `WaitPhaseNoWait WaitPhase = "no_wait"`, `WaitPhasePending = "pending"`, `WaitPhaseResolved = "resolved"`.
- `type WaitDiagnostics struct { Phase WaitPhase; Summary string; ExprResult bool; Truncated bool; Terms []WaitTermDiagnostic }` where `WaitTermDiagnostic{ Name, Kind, Label string; Result bool; Detail string }`.
- `type WorkflowWaitDiagnosticsOpts struct { SignalScanLimit int; IncludeAfterResolution bool }` (IncludeAfterResolution wired in Task 3).
- `func (w *Workflow[TTx]) WaitDiagnostics(ctx context.Context, taskName string, opts *WorkflowWaitDiagnosticsOpts) (*WaitDiagnostics, error)` (+ `WaitDiagnosticsTx`).

**Behavior:**
1. Load the workflow's tasks via `w.exec.JobGetWorkflowTasks` (or `w.LoadAll`), find the row whose `river:workflow_task` == taskName; error if not found.
2. If the task has no `river:workflow_wait` metadata → return `WaitDiagnostics{Phase: WaitPhaseNoWait}`.
3. `parseWaitSpec` from the metadata. Build `waiteval.Inputs`:
   - **Deps:** for each declared dep (from `river:workflow_deps`), find the sibling row → `DepView{Output: <parsed output metadata or nil>, State: row.State}`. Missing dep → present key, nil output.
   - **Signals:** `w.exec.WorkflowSignalList({WorkflowID, Max: scanLimit, OrderByNewest: true})`; build latest-per-key `SignalView`. Set `Truncated = len(signals) >= scanLimit`.
   - **Timers:** anchors — `WorkflowCreatedAt` from `workflowid.Timestamp(w.id)` (fallback task CreatedAt), `WaitStartedAt` from `river:workflow_wait_started_at` metadata (zero if absent → timers relative to wait-start simply not yet fired, which is the honest snapshot), `DepFinalizedAt` from sibling `FinalizedAt`. Resolve each timer term via `waiteval.ResolveTimer`.
4. `waiteval.Compile(spec.toEngineTerms(), spec.Expr)` then `EvaluateReport(inputs)`.
5. Map to `WaitDiagnostics`: `ExprResult` from the report; `Phase` = `WaitPhaseResolved` if ExprResult else `WaitPhasePending`; `Terms` from the report's per-term results (carry the term `Label` from the spec); `Summary` = a one-line human roll-up (e.g. "2/3 terms satisfied; expr=false").
   `SignalScanLimit` default 10_000, capped 100_000.

- [ ] **Step 1: Failing integration test** `diagnostics_test.go` (pgx + `riverdbtest.TestSchema` harness, copy from `signals_test.go`): build a workflow with a signal-gated task (`WaitTermSignal("approved","approved","payload.ok")`, `Expr:"approved"`), Prepare+InsertMany, do NOT emit yet → `WaitDiagnostics("gate", nil)` returns `Phase==WaitPhasePending`, `ExprResult==false`, the "approved" term `Result==false`. Then `Signals().Emit(...,{ok:true})` → `WaitDiagnostics` returns `Phase==WaitPhaseResolved`, `ExprResult==true`, term `Result==true`. Also a no-wait task → `WaitPhaseNoWait`.

- [ ] **Step 2: Implement** `diagnostics.go`. Reuse `parseWaitSpec`, `(*WaitSpec).toEngineTerms`, `waiteval`, `workflowid.Timestamp`, and the dep-output parsing already used by `WorkflowTasks.Output`. Keep input-building read-only.

- [ ] **Step 3: GREEN** — `cd riverworkflow && go test ./...` fully green; `go build ./...` clean; gofmt clean.

- [ ] **Step 4: Commit** `git commit -m "Add Workflow.WaitDiagnostics read-only wait snapshot"`

---

### Task 3: `IncludeAfterResolution` + `resolved_at` filtering

**Files:** Modify `riverworkflow/signals.go` (ListForTask/LatestForTask honor IncludeAfterResolution), `riverworkflow/diagnostics.go` (opt plumbed), possibly `riverdriver` `WorkflowSignalListParams` (add `IncludeResolved bool` + SQL `AND (resolved_at IS NULL OR @include_resolved)`).

**Interfaces:** `WorkflowSignalListForTaskParams{ IncludeAfterResolution bool }`, `WorkflowSignalLatestForTaskOpts{ IncludeAfterResolution bool }` honored; default (false) excludes signals whose `resolved_at` is set.

**Reality note (PARITY):** Nothing currently WRITES `resolved_at` (CP3 left it for here). Implement the read/filter semantics fully. For WRITING: when the scheduler promotes a signal-gated task, stamp `resolved_at` on the signals that satisfied it — BUT this requires identifying "which signals satisfied the wait", which Pro's internals define and we cannot verify. SCOPE DECISION for CP4: implement the filter (reads honor `resolved_at`) and a single, defensible writer — when `JobApplyWorkflowWait` promotes a wait task, mark all currently-unresolved signals for that workflow+the spec's signal keys as resolved (`resolved_at = now`). Mark this `// PARITY: resolution-marking semantics inferred; Pro may scope differently`. If this proves too speculative during implementation, STOP and report — it is acceptable to ship CP4 with the read/filter + opt plumbed and `resolved_at` writing DEFERRED with a clear doc note, rather than guess wrong.

- [ ] **Step 1: Failing test** — emit two signals for a key; mark one resolved (via whatever writer is implemented, or directly in the test by emitting+promoting); `ListForTask(..., {IncludeAfterResolution:false})` excludes the resolved one; `{IncludeAfterResolution:true}` includes both. Driver-level: extend the signal conformance with an `IncludeResolved` filter case if a driver param is added.

- [ ] **Step 2: Implement** the read filter (driver param + SQL `resolved_at` predicate, regen, conformance) and the opt plumbing through `signals.go`/`diagnostics.go`. Implement the writer per the scope decision above, or defer-with-doc if speculative.

- [ ] **Step 3: GREEN** across drivers (`make verify/sqlc` clean if SQL changed); `cd riverworkflow && go test ./...` green.

- [ ] **Step 4: Commit** `git commit -m "Honor IncludeAfterResolution via resolved_at filtering"`

---

### Task 4: End-to-end diagnostics example

**Files:** Create `riverworkflow/example_workflow_diagnostics_test.go`.

- [ ] **Step 1: Example** — build a signal-gated workflow, call `WaitDiagnostics` before emit (print the Phase, e.g. `pending`), emit, call again (print `resolved`), with a deterministic `// Output:` (two lines: `pending` then `resolved`). Follow the CP2/CP3 example style; 3× stable.

- [ ] **Step 2: Commit** `git commit -m "Add end-to-end WaitDiagnostics example"`

---

## Checkpoint exit criteria

- `waiteval.EvaluateReport` returns per-term results; existing behavior preserved.
- `Workflow.WaitDiagnostics` returns an accurate read-only snapshot: `WaitPhaseNoWait`/`Pending`/`Resolved`, `ExprResult`, per-term results, and `Truncated` when the signal scan hit the limit — proven by an integration test showing pending-before-emit and resolved-after-emit, agreeing with real scheduler behavior.
- `IncludeAfterResolution`/`resolved_at` reads honored across drivers (writer implemented or explicitly deferred-with-doc, marked PARITY).
- No mutation from the diagnostics path; no raw dialect SQL in `riverworkflow`; all three drivers exercised where SQL changed.

## Feature completion (after CP4 merges)

The wait-family is then complete: DAG (CP1) + timers/CEL (CP2) + signals (CP3) + diagnostics (CP4). Remaining out-of-scope per spec §14: River UI visualization (Pro-only OSS-UI gap), and the pre-existing `cancelExpiredWorkflows` Postgres-only deadline clause (separate ticket, flagged in CP2 review). Run a final whole-feature opus review across CP1–CP4 before considering it done.

## Self-review checklist (run after writing)
- Spec §10 WaitDiagnostics → Tasks 1–2; Truncated → Task 2; IncludeAfterResolution/resolved_at → Task 3; example → Task 4.
- Read-only guarantee stated and testable. No scheduler divergence (test asserts diagnostics matches actual pending/resolved). Dialect-safe reads only.
- Type names consistent: `WaitDiagnostics{Phase,Summary,ExprResult,Truncated,Terms}`, `WaitPhase` consts, `WorkflowWaitDiagnosticsOpts{SignalScanLimit,IncludeAfterResolution}`, `waiteval.EvaluateReport`/`EvalReport`/`TermReport`.
- PARITY honesty: `resolved_at` writing is inferred — implement defensibly or defer with a clear note; do not silently guess Pro internals.
