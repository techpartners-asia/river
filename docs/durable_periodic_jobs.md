# Durable Periodic Jobs

Durable periodic jobs persist each periodic job's next run time to the
`river_periodic_job` table so the schedule survives client restarts, crashes,
and leader elections. The standard (non-durable) periodic scheduler is
in-memory only: when a new leader is elected, all periodic jobs re-schedule
from "now," which can drop runs of long-interval jobs that span a restart.

This feature matches the [River Pro durable periodic jobs](https://riverqueue.com/docs/pro/durable-periodic-jobs)
API surface.

## Enabling

Set `Config.DurablePeriodicJobs.Enabled` and assign a non-empty
`PeriodicJobOpts.ID` to each periodic job you want to make durable. Jobs
without an `ID` remain in-memory even when the flag is enabled.

```go
riverClient, err := river.NewClient(
    riverpgxv5.New(dbPool),
    &river.Config{
        PeriodicJobs: []*river.PeriodicJob{
            river.NewPeriodicJob(
                river.PeriodicInterval(15*time.Minute),
                func() (river.JobArgs, *river.InsertOpts) {
                    return MyPeriodicJobArgs{}, nil
                },
                &river.PeriodicJobOpts{ID: "my_periodic_job"},
            ),
        },
        DurablePeriodicJobs: river.DurablePeriodicJobsConfig{Enabled: true},
        // Workers and Queues omitted for brevity.
    },
)
```

## Configuration

| Field            | Default | Description                                                                                                 |
| ---------------- | ------- | ----------------------------------------------------------------------------------------------------------- |
| `Enabled`        | `false` | Turns on durable scheduling for periodic jobs that have an `ID`.                                            |
| `StaleThreshold` | `24h`   | Duration after which an orphaned row (an `ID` no longer registered with any client) is reaped. Min: 1 min. |

## Behavior

| Aspect              | Behavior                                                                                                          |
| ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Hydration on start  | On leader start, the enqueuer reads persisted next run times and uses them instead of `ScheduleFunc(now)`.        |
| New job (no row)    | The first scheduling tick computes a next run time and upserts a row.                                             |
| `RunOnStart=true`   | Fires the job on every client start, regardless of any persisted next run time. Normal schedule resumes after.   |
| Schedule change     | Assign a new `ID` (convention: `_v2` suffix). The old row is reaped after `StaleThreshold`.                       |
| Keep-alive          | Every 10 minutes, currently-registered IDs have their `updated_at` refreshed.                                     |
| Reap                | Same tick deletes rows whose `id` is no longer registered AND whose `updated_at` is older than `StaleThreshold`.  |
| Leader election     | A new leader hydrates from the database and continues the schedule.                                               |
| Crash recovery      | Persisted next run time survives — no drop unless the row also exceeds `StaleThreshold` without keep-alive.       |
| `Enabled = false`   | No rows are written. Behavior identical to the in-memory scheduler.                                               |

## Schema

Migration `008_durable_periodic_jobs` adds the `river_periodic_job` table:

```sql
CREATE TABLE river_periodic_job (
    id text PRIMARY KEY NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    next_run_at timestamptz NOT NULL
);
```

(SQLite uses `timestamp` columns instead of `timestamptz`, matching the
existing River SQLite convention.)

## Notes

- Only the elected leader writes to `river_periodic_job`. Multiple clients
  pointing at the same database coordinate via leader election as usual.
- Periodic jobs that omit `ID` are unaffected: they stay in-memory whether
  or not `DurablePeriodicJobs.Enabled` is set.
- To switch a non-durable periodic job to durable, simply assign an `ID`
  and enable the config. The first run after the change schedules from
  `ScheduleFunc(now)` because no row exists yet.
