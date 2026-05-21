# Durable Periodic Jobs — Design

**Date:** 2026-05-21
**Branch:** `feature/riverworkflow` (or successor)
**Status:** Design approved, ready for implementation plan
**Parity target:** [River Pro durable periodic jobs](https://riverqueue.com/docs/pro/durable-periodic-jobs)

## 1. Goal

Reproduce River Pro's durable periodic jobs feature inside OSS so that periodic-job next-run-times persist across restarts, crashes, and leader elections. Behavior, configuration shape, and semantics match the Pro documentation exactly.

## 2. Background

The OSS enqueuer (`internal/maintenance/periodic_job_enqueuer.go`) already calls a `riverpilot.PilotPeriodicJob` trio on every relevant tick:

- `PeriodicJobGetAll` on service start to hydrate `NextRunAt` from durable storage.
- `PeriodicJobUpsertMany` on every scheduling tick to persist new `NextRunAt`.
- `PeriodicJobKeepAliveAndReap` on a 10-minute ticker to touch live rows and reap orphans.

`StandardPilot` (`rivershared/riverpilot/standard_pilot.go`) currently returns `nil, nil` from all three. The Pro pilot supplies a non-trivial implementation backed by a `river_periodic_job` table. This spec brings that implementation into OSS, gated by a config flag with Pro-identical surface.

## 3. Non-goals

- riverui visualization of the new table (separate PR).
- Multi-region / sharded coordination — same constraint as Pro: one cluster per database.
- Automatic schedule-fingerprinting. Pro convention is to bump the periodic job `ID` (`my_job_v2`); we keep that contract.
- Migration tooling for users upgrading from Pro to OSS (table shape is identical, so a manual rename suffices if needed; not in scope here).

## 4. Public API

### 4.1 Config

New struct on `river.Config`:

```go
type Config struct {
    // ...existing fields...

    // DurablePeriodicJobs enables durable scheduling for periodic jobs that
    // have an ID assigned. Next run times persist across restarts, crashes,
    // and leader elections.
    DurablePeriodicJobs DurablePeriodicJobsConfig
}

type DurablePeriodicJobsConfig struct {
    // Enabled turns on durable scheduling for any periodic job that has a
    // non-empty PeriodicJobOpts.ID. Periodic jobs without an ID stay in-memory
    // even when Enabled is true.
    Enabled bool

    // StaleThreshold is the duration after which a periodic job row whose ID
    // is no longer registered with any active client is considered orphaned
    // and deleted by PeriodicJobKeepAliveAndReap. Defaults to 24h. Must be at
    // least 1 minute when set explicitly.
    StaleThreshold time.Duration
}
```

Validation (in `Config.validate`):

- If `DurablePeriodicJobs.Enabled` and `StaleThreshold == 0` → set `StaleThreshold = 24 * time.Hour`.
- If `StaleThreshold > 0 && StaleThreshold < time.Minute` → return validation error.
- `DurablePeriodicJobs.Enabled = false` (zero value) keeps current OSS behavior unchanged.

### 4.2 Periodic job opts

Unchanged. `PeriodicJobOpts.ID` already exists; durability simply takes effect when `ID != "" && DurablePeriodicJobs.Enabled`.

### 4.3 Usage example (matches Pro doc)

```go
riverClient, err := river.NewClient(
    riverpgxv5.New(dbPool),
    &river.Config{
        PeriodicJobs: []*river.PeriodicJob{
            river.NewPeriodicJob(
                river.PeriodicInterval(15*time.Minute),
                func() (river.JobArgs, *river.InsertOpts) {
                    return PeriodicJobArgs{}, nil
                },
                &river.PeriodicJobOpts{ID: "my_periodic_job"},
            ),
        },
        DurablePeriodicJobs: river.DurablePeriodicJobsConfig{Enabled: true},
    },
)
```

## 5. Semantics (Pro-identical)

| Aspect | Behavior |
|---|---|
| Durability scope | Only periodic jobs with non-empty `ID` are durable. IDless jobs stay in-memory even when `Enabled = true`. |
| Hydration on start | Enqueuer reads all rows via pilot, maps `id → next_run_at`. For each configured job with a matching ID, uses persisted `NextRunAt` instead of `ScheduleFunc(now)`. |
| New job (no row) | Computes `NextRunAt = ScheduleFunc(now)` and upserts on first tick. |
| Tick scheduling | After firing, persists the next computed `NextRunAt`. Same transaction as the job insert (existing enqueuer behavior). |
| `RunOnStart=true` | Fires the job on leader start regardless of persisted `NextRunAt`. Then normal schedule continues from the persisted/computed `NextRunAt`. |
| Schedule change | User must assign a new `ID` (`my_job_v2` convention). Old row is reaped after `StaleThreshold`. |
| Keep-alive | Every 10 minutes (existing ticker), all currently-registered IDs have `updated_at` bumped to `NOW()`. Driver and pilot do this in one round-trip. |
| Reap | Same 10-minute tick deletes rows whose `id` is not in the registered set AND `updated_at < now - StaleThreshold`. |
| Leader election | Only the elected leader runs the enqueuer (unchanged). A new leader hydrates from DB and proceeds without skipping or duplicating runs beyond the standard `nowWithMargin` tolerance. |
| Crash recovery | Same as leader election — the persisted `NextRunAt` survives. |
| Disabled state | If `Enabled = false`, pilot returns `nil, nil` from all three methods. Enqueuer behaves identically to today. |

## 6. Architecture

### 6.1 Layers touched

```
river.Config                       ← new DurablePeriodicJobs field
  └─ river.Client                  ← passes config into StandardPilot at construction
     └─ riverpilot.StandardPilot   ← new durable behavior, gated by config
        └─ riverdriver.Executor    ← three new methods (per driver)
           └─ river_periodic_job   ← new table
```

The enqueuer itself is **untouched** — its existing pilot-trio calls are already correct.

### 6.2 `riverpilot.StandardPilot`

```go
type StandardPilot struct {
    seq            atomic.Int64
    durableEnabled bool
    staleThreshold time.Duration
    timeGen        baseservice.TimeGeneratorWithStub
}
```

`PilotInit` is extended to receive a durable config so external callers of `StandardPilot{}` literal continue to compile (zero value = disabled):

```go
type PilotInitParams struct {
    // ...existing fields...
    DurablePeriodicJobs DurablePeriodicJobsConfig
}
```

Method changes:

- `PeriodicJobGetAll`: if `!durableEnabled` → `nil, nil`; else `exec.PeriodicJobGetAll(ctx, params)`.
- `PeriodicJobUpsertMany`: filter `params.Jobs` to entries with non-empty `ID` (defensive — enqueuer already filters); if empty or `!durableEnabled` → `nil, nil`; else delegate.
- `PeriodicJobKeepAliveAndReap`: if `!durableEnabled` → `nil, nil`; else compute `staleHorizon := timeGen.NowUTC().Add(-staleThreshold)` and pass it to the executor along with `params.ID`.

### 6.3 `riverdriver.Executor` additions

```go
PeriodicJobGetAll(ctx context.Context, params *PeriodicJobGetAllParams) ([]*rivertype.PeriodicJob, error)
PeriodicJobUpsertMany(ctx context.Context, params *PeriodicJobUpsertManyParams) ([]*rivertype.PeriodicJob, error)
PeriodicJobKeepAliveAndReap(ctx context.Context, params *PeriodicJobKeepAliveAndReapParams) ([]*rivertype.PeriodicJob, error)

type PeriodicJobGetAllParams struct {
    Schema string
}

type PeriodicJobUpsertManyParams struct {
    Schema string
    Jobs   []PeriodicJobUpsertParams
}

type PeriodicJobUpsertParams struct {
    ID        string
    NextRunAt time.Time
    UpdatedAt time.Time
}

type PeriodicJobKeepAliveAndReapParams struct {
    Schema       string
    ID           []string
    StaleHorizon time.Time // rows with updated_at < StaleHorizon and id not in ID are deleted
}
```

`rivertype.PeriodicJob` is promoted from `riverpilot.PeriodicJob` (per existing TODO in `pilot.go`); the pilot type aliases the rivertype one to keep the Pro contract stable. Result slices use the rivertype struct.

### 6.4 Schema (`river_periodic_job`)

**Postgres (riverpgxv5 + riverdatabasesql)**

```sql
CREATE TABLE /* TEMPLATE: {{.Schema}}. */river_periodic_job (
    id          text PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT NOW(),
    updated_at  timestamptz NOT NULL DEFAULT NOW(),
    next_run_at timestamptz NOT NULL,
    CONSTRAINT id_length CHECK (char_length(id) > 0 AND char_length(id) < 128)
);
```

**SQLite (riversqlite)**

```sql
CREATE TABLE river_periodic_job (
    id          TEXT PRIMARY KEY NOT NULL,
    created_at  INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
    updated_at  INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
    next_run_at INTEGER NOT NULL,
    CHECK (length(id) > 0 AND length(id) < 128)
) STRICT;
```

Migration number `008_durable_periodic_jobs` in:

- `rivermigrate/migration/main/`
- `riverdriver/riverpgxv5/migration/main/`
- `riverdriver/riverdatabasesql/migration/main/`
- `riverdriver/riversqlite/migration/main/`

Each ships an `.up.sql` and a `.down.sql` (just `DROP TABLE river_periodic_job`).

### 6.5 Driver SQL

**`PeriodicJobGetAll`** (Postgres):

```sql
SELECT id, created_at, updated_at, next_run_at
FROM /* TEMPLATE: {{.Schema}}. */river_periodic_job
ORDER BY id;
```

**`PeriodicJobUpsertMany`** (Postgres, batched via `unnest`):

```sql
INSERT INTO /* TEMPLATE: {{.Schema}}. */river_periodic_job (id, next_run_at, updated_at)
SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::timestamptz[])
ON CONFLICT (id) DO UPDATE
SET next_run_at = EXCLUDED.next_run_at,
    updated_at  = EXCLUDED.updated_at
RETURNING id, created_at, updated_at, next_run_at;
```

SQLite variant uses `INSERT ... ON CONFLICT(id) DO UPDATE` and serializes the batch as JSON consumed via `json_each` (consistent with the workflow driver's approach in this branch).

**`PeriodicJobKeepAliveAndReap`** (Postgres, single statement, CTE):

```sql
WITH touched AS (
    UPDATE /* TEMPLATE: {{.Schema}}. */river_periodic_job
    SET updated_at = NOW()
    WHERE id = ANY($1::text[])
    RETURNING id
),
reaped AS (
    DELETE FROM /* TEMPLATE: {{.Schema}}. */river_periodic_job
    WHERE NOT (id = ANY($1::text[]))
      AND updated_at < $2::timestamptz
    RETURNING id, created_at, updated_at, next_run_at
)
SELECT id, created_at, updated_at, next_run_at FROM reaped;
```

SQLite uses two statements inside a transaction; arrays serialized as JSON, no native array type.

## 7. File-level change map

### New files

- `riverdriver/riverpgxv5/migration/main/008_durable_periodic_jobs.up.sql`
- `riverdriver/riverpgxv5/migration/main/008_durable_periodic_jobs.down.sql`
- `riverdriver/riverdatabasesql/migration/main/008_durable_periodic_jobs.up.sql`
- `riverdriver/riverdatabasesql/migration/main/008_durable_periodic_jobs.down.sql`
- `riverdriver/riversqlite/migration/main/008_durable_periodic_jobs.up.sql`
- `riverdriver/riversqlite/migration/main/008_durable_periodic_jobs.down.sql`
- `rivermigrate/migration/main/003_durable_periodic_jobs.up.sql` (next migration number in this set — confirm at impl time)
- `rivermigrate/migration/main/003_durable_periodic_jobs.down.sql`
- `riverdriver/riverpgxv5/internal/dbsqlc/periodic_job.sql` (sqlc input)
- `riverdriver/riverpgxv5/internal/dbsqlc/periodic_job.sql.go` (sqlc output)
- `riverdriver/riverdatabasesql/internal/dbsqlc/periodic_job.sql` + generated
- `riverdriver/riversqlite/periodic_job.go` (hand-written, mirrors workflow driver style)
- `rivershared/riverpilot/standard_pilot_durable_test.go`
- `rivertest/.../periodic_job_durable_test.go` end-to-end coverage if a separate file is preferred — otherwise extend `client_test.go`.
- `example_durable_periodic_job_test.go`
- `docs/durable_periodic_jobs.md`

### Modified files

- `rivershared/riverpilot/pilot.go` — promote `PeriodicJob` to `rivertype.PeriodicJob` (alias for back-compat), extend `PilotInitParams` with `DurablePeriodicJobs DurablePeriodicJobsConfig` (or use a narrow internal type to avoid an import cycle; spec author will resolve at impl time).
- `rivershared/riverpilot/standard_pilot.go` — durable fields + non-no-op implementations.
- `riverdriver/executor.go` (or wherever `Executor` is defined) — add the three methods.
- `riverdriver/riverdrivertest/conformance.go` — add periodic-job conformance bundle.
- `riverdriver/riverpgxv5/river_pgx_v5_driver.go` — implement new methods.
- `riverdriver/riverdatabasesql/river_database_sql_driver.go` — implement new methods.
- `riverdriver/riversqlite/river_sqlite_driver.go` — implement new methods.
- `client.go` — pass `config.DurablePeriodicJobs` into `StandardPilot` via `PilotInit`.
- `config.go` (or wherever `Config.validate` lives) — validate `DurablePeriodicJobs`.
- `periodic_job.go` — doc comment on `NewPeriodicJob` mentions `DurablePeriodicJobsConfig`.
- `rivertype/periodic_job.go` (new struct file or appended) — `PeriodicJob` type.
- `CHANGELOG.md` — entry under unreleased.

## 8. Testing strategy

### 8.1 Unit — pilot

`rivershared/riverpilot/standard_pilot_durable_test.go`:

- `Enabled = false`: all three methods return `nil, nil`, executor mock receives zero calls.
- `Enabled = true, ID = ""` jobs in upsert input: filtered out; executor receives only IDed entries.
- `Enabled = true`: get/upsert/reap delegate to executor with expected params; `StaleHorizon = now - threshold` computed against the stubbed time generator.

### 8.2 Unit — enqueuer

Extend `internal/maintenance/periodic_job_enqueuer_test.go`:

- Hydration: `PilotMock.PeriodicJobGetAll` returns `{ID: "j", NextRunAt: now+30m}`. Configure a 1h interval. Assert enqueuer's first scheduled run uses `now+30m`, not `now+1h`.
- Hydration ignored for unknown ID: `Get` returns `{ID: "old"}` but config has only `"new"`. Old ID is left to be reaped; new job schedules via `ScheduleFunc(now)`.
- RunOnStart with persisted `NextRunAt`: enqueuer fires job on start AND keeps persisted `NextRunAt` for subsequent scheduling.
- Upsert filtering: jobs without `ID` never appear in `PeriodicJobUpsertMany` params (already enforced in current code — assert by inspection).

### 8.3 Driver conformance

Extend `riverdriver/riverdrivertest`:

- `PeriodicJob_Upsert_InsertAndUpdate`: first upsert sets row, second upsert (same ID) updates `next_run_at` + `updated_at`, leaves `created_at` unchanged.
- `PeriodicJob_GetAll`: returns inserted rows ordered by id.
- `PeriodicJob_KeepAliveAndReap_BumpsUpdatedAt`: registered IDs see `updated_at` advance.
- `PeriodicJob_KeepAliveAndReap_DeletesOrphans`: row not in `ID` and `updated_at < staleHorizon` is deleted; row not in `ID` but recent is kept; row in `ID` is never deleted.
- Runs against all three drivers via the existing conformance harness.

### 8.4 End-to-end

In `client_test.go` (or a new `durable_periodic_job_test.go`):

- Start client, `DurablePeriodicJobs.Enabled = true`, one periodic job `ID: "test_job"` 1h interval. Wait for first scheduling tick (uses `riversharedtest` time stubbing). Stop client. Assert row exists with computed `NextRunAt`.
- Restart client with same config. Assert enqueuer reuses persisted `NextRunAt` (verify via `PeriodicJobGetAll` test signal — already exists in `client_pilot_test.go`).
- Same flow with `Enabled = false` → assert table stays empty.

### 8.5 Migration

- `rivermigrate` test that applies `up` then `down` cleanly, verifying table exists then is dropped.
- Cross-driver smoke (already covered by the migration test harness if it iterates drivers).

## 9. Documentation

- `docs/durable_periodic_jobs.md` — overview, config example, behavior table (mirror the Pro doc structure but use OSS imports). Cross-link from `docs/README.md` if one exists.
- `periodic_job.go` doc comment on `NewPeriodicJob`: append a sentence — "When `Config.DurablePeriodicJobs.Enabled` is set and the job has a non-empty `ID`, its next run time persists across restarts."
- `CHANGELOG.md`:
  ```
  ### Added
  - Durable periodic jobs that persist next run times across restarts via
    `Config.DurablePeriodicJobs`. Periodic jobs with a non-empty `ID` become
    durable when enabled. Matches the River Pro feature surface.
  ```
- `example_durable_periodic_job_test.go` — runnable example matching the Pro snippet.

## 10. Rollout

- New table, new methods — purely additive. No existing code path changes when `Enabled = false`.
- Existing OSS users on `Enabled = false` (default): no behavioral change.
- Users running both River OSS clusters and River Pro against the same DB: the table shape is identical, so they coexist. Schema migration must be applied exactly once (operator concern; documented).

## 11. Risks and open items

| Risk | Mitigation |
|---|---|
| Import cycle when threading `DurablePeriodicJobsConfig` into `riverpilot` | Use a narrow internal struct defined in `riverpilot` (`type DurableConfig struct { Enabled bool; StaleThreshold time.Duration }`); top-level `Config` translates into it during `PilotInit`. |
| Existing Pro callers constructing `StandardPilot{}` literal | Zero-value behavior is preserved (`durableEnabled = false`). Only the `PilotInit` path activates durability. |
| `rivertype.PeriodicJob` migration breaking Pro | Keep `riverpilot.PeriodicJob` as an alias of the rivertype struct; field names identical. Pro continues to compile. |
| Migration number collision | Confirm next free number per driver at implementation time. Current branch already has `007_workflow_index`, so `008` is the candidate. The `rivermigrate/migration/main` set is on `002`, so its next free number is `003`. |
| Stale-threshold lower bound | Validation rejects `< 1m` to avoid reaping in-flight clients between keep-alive ticks (10m). |
| Time-stub plumbing in pilot | Use `baseservice.TimeGeneratorWithStub` already used elsewhere; injected through `PilotInit`. |

## 12. Out of scope

- riverui display of `river_periodic_job` rows.
- Inserting via dynamic API (`Add` / `AddSafely`) when leader is not local — already a Pro/OSS shared limitation; no change.
- Multi-tenant per-schema isolation beyond the existing `Schema` plumbing — already supported.

## 13. Acceptance criteria

1. Configuring `DurablePeriodicJobs.Enabled = true` with a periodic job `ID: "x"` results in a `river_periodic_job` row after the first scheduling tick, with `next_run_at` matching the enqueuer's plan.
2. Restarting the client preserves the next run time (assertable via test signal).
3. `RunOnStart = true` fires the job on every client start regardless of persisted `next_run_at`.
4. Removing a periodic job from config and leaving the client running for `StaleThreshold` causes the orphaned row to be deleted on the next keep-alive tick.
5. With `DurablePeriodicJobs.Enabled = false`, no rows are ever written and behavior matches today's OSS.
6. All three drivers (pgxv5, databasesql, sqlite) pass the new conformance bundle.
7. `up` then `down` migration round-trip is clean for all three drivers.
