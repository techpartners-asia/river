# Workflow waits: signals, timers, and CEL conditions

This fork extends River's OSS workflows (fan-out / fan-in DAGs in
[`riverworkflow`](../riverworkflow)) with a **wait family** that mirrors River
Pro's documented workflow-wait surface:

- **Signals** — external / human-in-the-loop events that unblock a task.
- **Timers** — a task waits until a wall-clock instant or a duration after an anchor.
- **CEL conditions** — a boolean [CEL](https://github.com/google/cel-spec)
  expression over signals, timers, and dependency outputs gates a task.
- **Diagnostics** — a read-only snapshot of *why* a task is (not) ready.

A wait is **layered on top of** the existing dependency mechanism: a
wait-bearing task is promoted only when **(its dependencies are satisfied)
AND (its wait expression resolves true)**.

> Storage note: signals require the `river_workflow_signal` table added in
> migration `009`. Run your migrations (the fork's `cmd/river migrate-up` or
> `rivermigrate`) before using signals.

## Attaching a wait to a task

Pass a `*WaitSpec` via `WorkflowTaskOpts.Wait`. A `WaitSpec` is a set of named
**terms** plus a CEL `Expr` that combines them by name:

```go
wf := workflowClient.NewWorkflow(&riverworkflow.WorkflowOpts{Name: "Approval"})
wf.Add("fetch", FetchArgs{}, nil, nil)
wf.Add("approve", ApproveArgs{}, nil, &riverworkflow.WorkflowTaskOpts{
    Deps: []string{"fetch"},
    Wait: &riverworkflow.WaitSpec{
        Terms: []riverworkflow.WaitTermSpec{
            riverworkflow.WaitTermSignal("approved", "approved", "payload.ok").
                Label("Manager approval"),
        },
        Expr: "approved", // promote once the "approved" term is true
    },
})
res, _ := wf.Prepare(ctx)
riverClient.InsertMany(ctx, res.Jobs)
```

`WaitSpec.Validate()` runs inside `Prepare`/`PrepareTx` and rejects malformed
CEL, unknown term references, duplicate term names, and bad timer anchors —
so a broken wait fails at insert time, not silently at runtime.

### Term builders

| Builder | Kind | Notes |
|---|---|---|
| `WaitTermSignal(name, key, celExpr)` | signal | True when a signal with `key` exists whose payload satisfies `celExpr` (e.g. `payload.ok`). An empty `celExpr` gates on signal *presence* alone. |
| `WaitTermTimer(TimerSpec)` | timer | True once the timer's fire time has passed. |
| `WaitTerm(name, celExpr)` | generic | A raw CEL boolean over the full input scope. |

Add a human label with `.Label("…")`.

### CEL variable scopes

- **Signal-term `celExpr`**: `payload`, `attempt`, `created_at`, `id`, `source`.
- **Generic `Expr` / `WaitTerm`**: `signals["k"]`, `timers["name"]`,
  `deps["task"].output`, and each declared term name as a bool.

A CEL expression that references not-yet-available data (an absent signal, a
dep whose output isn't present yet) evaluates to **false** ("not ready"), not
an error — the task simply stays pending and is re-evaluated on the next tick.

## Timers

Timers need no storage; their fire time is computed from an anchor:

| Builder | Fires at |
|---|---|
| `TimerAt(name, t)` | the absolute instant `t` |
| `TimerAfterWaitStarted(name, d)` | wait-evaluation start + `d` |
| `TimerAfterWorkflowCreated(name, d)` | workflow creation (decoded from the workflow-id ULID) + `d` |
| `TimerAfterTaskFinalized(name, dep, d)` | dependency `dep`'s `finalized_at` + `d` |

The scheduler polls timer-bearing waits at `WorkflowTimerPollerInterval`
(default 1s).

## Signals

Signals are workflow-scoped, queryable, and idempotent.

```go
sigs := wf.Signals() // or w.Signals() on a WorkflowFromExisting handle
sigs.Emit(ctx, "approved", map[string]any{"ok": true},
    &riverworkflow.WorkflowSignalEmitOpts{IdempotencyKey: "req-42", Source: "console"})

sigs.LatestForTask(ctx, "approve", "approved", nil) // newest matching signal
sigs.ListForTask(ctx, "approve", "approved", nil)   // history (excludes resolved by default)
sigs.List(ctx, nil)                                  // full workflow audit
```

- **Idempotency**: re-emitting with the same `IdempotencyKey` + identical
  payload is a no-op; with a *different* payload it returns
  `ErrSignalPayloadMismatch`.
- **Resolution**: when a signal-gated task is promoted, the signals that
  satisfied it are stamped `resolved_at`. `IncludeAfterResolution` controls
  whether resolved signals appear in `ListForTask`/`LatestForTask` (default:
  excluded). `List` is the full audit view and always includes them.

## Diagnostics

`WaitDiagnostics` re-evaluates a task's wait read-only (no promotion, no
writes) and reports why it is or isn't ready:

```go
diag, _ := w.WaitDiagnostics(ctx, "approve", nil)
// diag.Phase      -> WaitPhasePending | WaitPhaseResolved | WaitPhaseNoWait
// diag.ExprResult -> current boolean value of Expr
// diag.Terms      -> per-term results
// diag.Truncated  -> true if the signal scan hit SignalScanLimit
```

For read-only consumers that only have a driver executor (e.g. a dashboard),
use the package-level helper — no `Workflow` handle or scheduler required:

```go
diag, _ := riverworkflow.WaitDiagnosticsForExec(ctx, exec, schema, workflowID, "approve", nil)
```

## How the scheduler drives waits

The leader-elected `WorkflowScheduler` (installed by
`riverworkflow.NewClient`) runs each tick:

1. Promote dependency-only tasks via SQL (wait-bearing tasks are skipped by
   that fast path).
2. For each pending wait-bearing task whose dependencies are satisfied:
   classify dependencies (a failed dep cancels the task), load its signals
   (newest-first, bounded by `SignalScanLimit`), resolve its timers, evaluate
   the CEL, and **promote** on true.

Because evaluation runs in Go (CEL can't run in SQL), waits are held in the
database (`pending`) and released only by a running scheduler. **A read-only
client — such as the [riverui](https://github.com/techpartners-asia/riverui)
dashboard — does not run a scheduler and will not advance workflows.** You
need a worker process that calls `riverworkflow.NewClient(...).Start()` with
your workers registered.

## Driver / dialect support

All wait-family driver methods and the `009` migration are implemented and
conformance-tested across `riverpgxv5`, `riverdatabasesql`, and `riversqlite`.

## Known limitations

- River Pro is closed-source; the public API is mirrored from its docs, but
  some internal behaviors (e.g. the exact set of signals marked resolved on
  promotion) are inferred and marked `// PARITY:` in the code.
- River UI visualization lives in the separate
  [riverui](https://github.com/techpartners-asia/riverui) project; see its
  `docs/workflow_wait_ui.md`.
