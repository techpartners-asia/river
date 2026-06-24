CREATE TYPE river_job_state AS ENUM(
    'available',
    'cancelled',
    'completed',
    'discarded',
    'pending',
    'retryable',
    'running',
    'scheduled'
);

CREATE TABLE river_job (
    id bigserial PRIMARY KEY,
    args jsonb NOT NULL DEFAULT '{}',
    attempt smallint NOT NULL DEFAULT 0,
    attempted_at timestamptz,
    attempted_by text[],
    created_at timestamptz NOT NULL DEFAULT now(),
    errors jsonb[],
    finalized_at timestamptz,
    kind text NOT NULL,
    max_attempts smallint NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    priority smallint NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default',
    state river_job_state NOT NULL DEFAULT 'available',
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    tags varchar(255)[] NOT NULL DEFAULT '{}',
    unique_key bytea,
    unique_states bit(8),
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
    ),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

-- name: JobCancel :one
WITH locked_job AS (
    SELECT
        id, queue, state, finalized_at
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.id = @id
    FOR UPDATE
),
notification AS (
    SELECT
        id,
        pg_notify(
            concat(coalesce(sqlc.narg('schema')::text, current_schema()), '.', @control_topic::text),
            json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text
        )
    FROM
        locked_job
    WHERE
        state NOT IN ('cancelled', 'completed', 'discarded')
        AND finalized_at IS NULL
),
updated_job AS (
    UPDATE /* TEMPLATE: schema */river_job
    SET
        -- If the job is actively running, we want to let its current client and
        -- producer handle the cancellation. Otherwise, immediately cancel it.
        state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
        finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE coalesce(sqlc.narg('now')::timestamptz, now()) END,
        -- Mark the job as cancelled by query so that the rescuer knows not to
        -- rescue it, even if it gets stuck in the running state:
        metadata = jsonb_set(metadata, '{cancel_attempted_at}'::text[], @cancel_attempted_at::jsonb, true)
    FROM notification
    WHERE river_job.id = notification.id
    RETURNING river_job.*
)
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id = @id::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT *
FROM updated_job;

-- name: JobCountByAllStates :many
SELECT state, count(*)
FROM /* TEMPLATE: schema */ river_job
GROUP BY state;

-- name: JobCountByQueueAndState :many
WITH all_queues AS (
    SELECT DISTINCT unnest(@queue_names::text[])::text AS queue
),

running_job_counts AS (
    SELECT
        queue,
        COUNT(*) AS count
    FROM /* TEMPLATE: schema */river_job
    WHERE queue = ANY(@queue_names::text[])
        AND state = 'running'
    GROUP BY queue
),

available_job_counts AS (
    SELECT
        queue,
        COUNT(*) AS count
    FROM
      /* TEMPLATE: schema */river_job
    WHERE queue = ANY(@queue_names::text[])
        AND state = 'available'
    GROUP BY queue
)

SELECT
    all_queues.queue,
    COALESCE(available_job_counts.count, 0) AS count_available,
    COALESCE(running_job_counts.count, 0) AS count_running
FROM
    all_queues
LEFT JOIN
    running_job_counts ON all_queues.queue = running_job_counts.queue
LEFT JOIN
    available_job_counts ON all_queues.queue = available_job_counts.queue
ORDER BY all_queues.queue ASC;

-- name: JobCountByState :one
SELECT count(*)
FROM /* TEMPLATE: schema */river_job
WHERE state = @state;

-- name: JobDelete :one
WITH job_to_delete AS (
    SELECT id
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.id = @id
    FOR UPDATE
),
deleted_job AS (
    DELETE
    FROM /* TEMPLATE: schema */river_job
    USING job_to_delete
    WHERE river_job.id = job_to_delete.id
        -- Do not touch running jobs:
        AND river_job.state != 'running'
    RETURNING river_job.*
)
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id = @id::bigint
    AND id NOT IN (SELECT id FROM deleted_job)
UNION
SELECT *
FROM deleted_job;

-- name: JobDeleteBefore :execresult
DELETE FROM /* TEMPLATE: schema */river_job
WHERE id IN (
    SELECT id
    FROM /* TEMPLATE: schema */river_job
    WHERE (
            (state = 'cancelled' AND @cancelled_do_delete AND finalized_at < @cancelled_finalized_at_horizon::timestamptz) OR
            (state = 'completed' AND @completed_do_delete AND finalized_at < @completed_finalized_at_horizon::timestamptz) OR
            (state = 'discarded' AND @discarded_do_delete AND finalized_at < @discarded_finalized_at_horizon::timestamptz)
        )
        AND (
            @queues_excluded::text[] IS NULL
            OR NOT (queue = any(@queues_excluded))
        )
        AND (
            @queues_included::text[] IS NULL
            OR queue = any(@queues_included)
        )
    ORDER BY id
    LIMIT @max::bigint
);

-- name: JobDeleteMany :many
WITH jobs_to_delete AS (
    SELECT *
    FROM /* TEMPLATE: schema */river_job
    WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
        AND state != 'running'
    ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
    LIMIT @max::int
    FOR UPDATE
    SKIP LOCKED
),
deleted_jobs AS (
    DELETE FROM /* TEMPLATE: schema */river_job
    WHERE id IN (SELECT id FROM jobs_to_delete)
    RETURNING *
)
-- this last SELECT step is necessary because there's no other way to define
-- order records come back from a DELETE statement
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id IN (SELECT id FROM deleted_jobs)
ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */;

-- name: JobGetAvailable :many
WITH locked_jobs AS (
    SELECT
        *
    FROM
        /* TEMPLATE: schema */river_job
    WHERE
        state = 'available'
        AND queue = @queue::text
        AND scheduled_at <= coalesce(sqlc.narg('now')::timestamptz, now())
    ORDER BY
        priority ASC,
        scheduled_at ASC,
        id ASC
    LIMIT @max_to_lock::integer
    FOR UPDATE
    SKIP LOCKED
)
UPDATE
    /* TEMPLATE: schema */river_job
SET
    state = 'running',
    attempt = river_job.attempt + 1,
    attempted_at = coalesce(sqlc.narg('now')::timestamptz, now()),
    attempted_by = array_append(
        CASE WHEN array_length(river_job.attempted_by, 1) >= @max_attempted_by::int
        -- +2 instead of +1 because Postgres array indexing starts at 1, not 0.
        THEN river_job.attempted_by[array_length(river_job.attempted_by, 1) + 2 - @max_attempted_by:]
        ELSE river_job.attempted_by
        END,
        @attempted_by::text
    )
FROM
    locked_jobs
WHERE
    river_job.id = locked_jobs.id
RETURNING
    river_job.*;

-- name: JobGetByID :one
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id = @id
LIMIT 1;

-- name: JobGetByIDMany :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id = any(@id::bigint[])
ORDER BY id;

-- name: JobGetByKindMany :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE kind = any(@kind::text[])
ORDER BY id;

-- name: JobGetStuck :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE state = 'running'
    AND attempted_at < @stuck_horizon::timestamptz
ORDER BY id
LIMIT @max;

-- name: JobInsertFastMany :many
WITH raw_job_data AS (
    SELECT
        unnest(@id::bigint[]) AS id,
        unnest(@args::jsonb[]) AS args,
        unnest(@created_at::timestamptz[]) AS created_at,
        unnest(@kind::text[]) AS kind,
        unnest(@max_attempts::smallint[]) AS max_attempts,
        unnest(@metadata::jsonb[]) AS metadata,
        unnest(@priority::smallint[]) AS priority,
        unnest(@queue::text[]) AS queue,
        unnest(@scheduled_at::timestamptz[]) AS scheduled_at,
        unnest(@state::text[]) AS state,
        unnest(@tags::text[]) AS tags,
        unnest(@unique_key::bytea[]) AS unique_key,
        unnest(@unique_states::integer[]) AS unique_states
)
INSERT INTO /* TEMPLATE: schema */river_job(
    id,
    args,
    created_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) SELECT
    coalesce(nullif(id, 0), nextval('/* TEMPLATE: schema */river_job_id_seq'::regclass)),
    args,
    coalesce(nullif(created_at, '0001-01-01 00:00:00 +0000'), now()) AS created_at,
    kind,
    max_attempts,
    coalesce(metadata, '{}'::jsonb) AS metadata,
    priority,
    queue,
    coalesce(nullif(scheduled_at, '0001-01-01 00:00:00 +0000'), now()) AS scheduled_at,
    state::/* TEMPLATE: schema */river_job_state,
    string_to_array(tags, ',')::varchar(255)[],
    -- `nullif` is required for `lib/pq`, which doesn't do a good job of reading
    -- `nil` into `bytea`. We use `text` because otherwise `lib/pq` will encode
    -- to Postgres binary like `\xAAAA`.
    nullif(unique_key, '')::bytea,
    nullif(unique_states::integer, 0)::bit(8)
FROM raw_job_data
ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
        AND unique_states IS NOT NULL
        AND /* TEMPLATE: schema */river_job_state_in_bitmask(unique_states, state)
    -- Something needs to be updated for a row to be returned on a conflict.
    DO UPDATE SET kind = EXCLUDED.kind
RETURNING sqlc.embed(river_job), (xmax != 0) AS unique_skipped_as_duplicate;

-- name: JobInsertFastManyNoReturning :execrows
INSERT INTO /* TEMPLATE: schema */river_job(
    args,
    created_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) SELECT
    unnest(@args::jsonb[]),
    unnest(@created_at::timestamptz[]),
    unnest(@kind::text[]),
    unnest(@max_attempts::smallint[]),
    unnest(@metadata::jsonb[]),
    unnest(@priority::smallint[]),
    unnest(@queue::text[]),
    unnest(@scheduled_at::timestamptz[]),
    unnest(@state::/* TEMPLATE: schema */river_job_state[]),

    -- lib/pq really, REALLY does not play nicely with multi-dimensional arrays,
    -- so instead we pack each set of tags into a string, send them through,
    -- then unpack them here into an array to put in each row. This isn't
    -- necessary in the Pgx driver where copyfrom is used instead.
    string_to_array(unnest(@tags::text[]), ','),

    nullif(unnest(@unique_key::bytea[]), ''),
    nullif(unnest(@unique_states::integer[]), 0)::bit(8)
ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
        AND unique_states IS NOT NULL
        AND /* TEMPLATE: schema */river_job_state_in_bitmask(unique_states, state)
DO NOTHING;

-- name: JobInsertFull :one
INSERT INTO /* TEMPLATE: schema */river_job(
    args,
    attempt,
    attempted_at,
    attempted_by,
    created_at,
    errors,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) VALUES (
    @args::jsonb,
    coalesce(@attempt::smallint, 0),
    @attempted_at,
    @attempted_by,
    coalesce(sqlc.narg('created_at')::timestamptz, now()),
    @errors,
    @finalized_at,
    @kind,
    @max_attempts::smallint,
    coalesce(@metadata::jsonb, '{}'),
    @priority,
    @queue,
    coalesce(sqlc.narg('scheduled_at')::timestamptz, now()),
    @state::/* TEMPLATE: schema */river_job_state,
    coalesce(@tags::varchar(255)[], '{}'),
    -- `nullif` is required for `lib/pq`, which doesn't do a good job of reading
    -- `nil` into `bytea`. We use `text` because otherwise `lib/pq` will encode
    -- to Postgres binary like `\xAAAA`.
    nullif(@unique_key::text, '')::bytea,
    nullif(@unique_states::integer, 0)::bit(8)
) RETURNING *;

-- name: JobInsertFullMany :many
WITH raw_job_data AS (
    SELECT
        unnest(@args::jsonb[]) AS args,
        unnest(@attempt::smallint[]) AS attempt,
        unnest(@attempted_at::timestamptz[]) AS attempted_at,
        unnest(@created_at::timestamptz[]) AS created_at,
        unnest(@finalized_at::timestamptz[]) AS finalized_at,
        unnest(@kind::text[]) AS kind,
        unnest(@max_attempts::smallint[]) AS max_attempts,
        unnest(@metadata::jsonb[]) AS metadata,
        unnest(@priority::smallint[]) AS priority,
        unnest(@queue::text[]) AS queue,
        unnest(@scheduled_at::timestamptz[]) AS scheduled_at,
        unnest(@state::text[]) AS state,
        unnest(@tags::text[]) AS tags,
        unnest(@unique_key::text[]) AS unique_key,
        unnest(@unique_states::integer[]) AS unique_states
)
INSERT INTO /* TEMPLATE: schema */river_job(
    args,
    attempt,
    attempted_at,
    created_at,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
)
SELECT
    args,
    coalesce(attempt, 0) AS attempt,
    coalesce(nullif(attempted_at, '0001-01-01 00:00:00 +0000'), now()) AS attempted_at,
    coalesce(nullif(created_at, '0001-01-01 00:00:00 +0000'), now()) AS created_at,
    nullif(finalized_at, '0001-01-01 00:00:00 +0000') AS finalized_at,
    kind,
    max_attempts,
    coalesce(metadata, '{}'::jsonb) AS metadata,
    priority,
    queue,
    coalesce(nullif(scheduled_at, '0001-01-01 00:00:00 +0000'), now()) AS scheduled_at,
    state::/* TEMPLATE: schema */river_job_state,
    string_to_array(tags, ',')::varchar(255)[],
    -- `nullif` is required for `lib/pq`, which doesn't do a good job of reading
    -- `nil` into `bytea`. We use `text` because otherwise `lib/pq` will encode
    -- to Postgres binary like `\xAAAA`.
    nullif(unique_key, '')::bytea,
    nullif(unique_states::integer, 0)::bit(8)
FROM raw_job_data
RETURNING *;

-- name: JobKindList :many
SELECT DISTINCT ON (kind) kind
FROM /* TEMPLATE: schema */river_job
WHERE (@match = '' OR kind ILIKE '%' || @match || '%')
    AND (@after = '' OR kind > @after)
    AND (@exclude::text[] IS NULL OR kind != ALL(@exclude))
ORDER BY kind ASC
LIMIT @max;

-- name: JobList :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
LIMIT @max::int;

-- Run by the rescuer to queue for retry or discard depending on job state.
-- name: JobRescueMany :exec
UPDATE /* TEMPLATE: schema */river_job
SET
    errors = array_append(errors, updated_job.error),
    finalized_at = updated_job.finalized_at,
    scheduled_at = updated_job.scheduled_at,
    metadata = river_job.metadata || jsonb_build_object(
        'river:rescue_count',
        coalesce(
            CASE
                WHEN jsonb_typeof(river_job.metadata -> 'river:rescue_count') = 'number'
                    THEN (river_job.metadata ->> 'river:rescue_count')::int
            END,
            0
        ) + 1
    ),
    state = updated_job.state
FROM (
    SELECT
        unnest(@id::bigint[]) AS id,
        unnest(@error::jsonb[]) AS error,
        nullif(unnest(@finalized_at::timestamptz[]), '0001-01-01 00:00:00 +0000') AS finalized_at,
        unnest(@scheduled_at::timestamptz[]) AS scheduled_at,
        unnest(@state::text[])::/* TEMPLATE: schema */river_job_state AS state
) AS updated_job
WHERE river_job.id = updated_job.id;

-- name: JobRetry :one
WITH job_to_update AS (
    SELECT id
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.id = @id
    FOR UPDATE
),
updated_job AS (
    UPDATE /* TEMPLATE: schema */river_job
    SET
        state = 'available',
        max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
        finalized_at = NULL,
        scheduled_at = coalesce(sqlc.narg('now')::timestamptz, now())
    FROM job_to_update
    WHERE river_job.id = job_to_update.id
        -- Do not touch running jobs:
        AND river_job.state != 'running'
        -- If the job is already available with a prior scheduled_at, leave it alone.
        AND NOT (
            river_job.state = 'available'
            AND river_job.scheduled_at < coalesce(sqlc.narg('now')::timestamptz, now())
        )
    RETURNING river_job.*
)
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE id = @id::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT *
FROM updated_job;

-- name: JobSchedule :many
WITH jobs_to_schedule AS (
    SELECT
        id,
        unique_key,
        unique_states,
        priority,
        scheduled_at
    FROM /* TEMPLATE: schema */river_job
    WHERE
        state IN ('retryable', 'scheduled')
        AND priority >= 0
        AND queue IS NOT NULL
        AND scheduled_at <= coalesce(sqlc.narg('now')::timestamptz, now())
    ORDER BY
        priority,
        scheduled_at,
        id
    LIMIT @max::bigint
    FOR UPDATE
),
jobs_with_rownum AS (
    SELECT
        *,
        CASE
            WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL THEN
                ROW_NUMBER() OVER (
                    PARTITION BY unique_key
                    ORDER BY priority, scheduled_at, id
                )
            ELSE NULL
        END AS row_num
    FROM jobs_to_schedule
),
unique_conflicts AS (
    SELECT river_job.unique_key
    FROM /* TEMPLATE: schema */river_job
    JOIN jobs_with_rownum
        ON river_job.unique_key = jobs_with_rownum.unique_key
        AND river_job.id != jobs_with_rownum.id
    WHERE
        river_job.unique_key IS NOT NULL
        AND river_job.unique_states IS NOT NULL
        AND /* TEMPLATE: schema */river_job_state_in_bitmask(river_job.unique_states, river_job.state)
),
job_updates AS (
    SELECT
        job.id,
        job.unique_key,
        job.unique_states,
        CASE
            WHEN job.row_num IS NULL THEN 'available'::/* TEMPLATE: schema */river_job_state
            WHEN uc.unique_key IS NOT NULL THEN 'discarded'::/* TEMPLATE: schema */river_job_state
            WHEN job.row_num = 1 THEN 'available'::/* TEMPLATE: schema */river_job_state
            ELSE 'discarded'::/* TEMPLATE: schema */river_job_state
        END AS new_state,
        (job.row_num IS NOT NULL AND (uc.unique_key IS NOT NULL OR job.row_num > 1)) AS finalized_at_do_update,
        (job.row_num IS NOT NULL AND (uc.unique_key IS NOT NULL OR job.row_num > 1)) AS metadata_do_update
    FROM jobs_with_rownum job
    LEFT JOIN unique_conflicts uc ON job.unique_key = uc.unique_key
),
updated_jobs AS (
    UPDATE /* TEMPLATE: schema */river_job
    SET
        state        = job_updates.new_state,
        finalized_at = CASE WHEN job_updates.finalized_at_do_update THEN coalesce(sqlc.narg('now')::timestamptz, now())
                            ELSE river_job.finalized_at END,
        metadata     = CASE WHEN job_updates.metadata_do_update THEN river_job.metadata || '{"unique_key_conflict": "scheduler_discarded"}'::jsonb
                            ELSE river_job.metadata END
    FROM job_updates
    WHERE river_job.id = job_updates.id
    RETURNING
        river_job.id,
        job_updates.new_state = 'discarded'::/* TEMPLATE: schema */river_job_state AS conflict_discarded
)
SELECT
    sqlc.embed(river_job),
    updated_jobs.conflict_discarded
FROM /* TEMPLATE: schema */river_job
JOIN updated_jobs ON river_job.id = updated_jobs.id;

-- name: JobSetStateIfRunningMany :many
WITH job_input AS (
    SELECT
        unnest(@ids::bigint[])                     AS id,
        unnest(@attempt_do_update::boolean[])      AS attempt_do_update,
        unnest(@attempt::int[])                    AS attempt,
        unnest(@errors_do_update::boolean[])       AS errors_do_update,
        unnest(@errors::jsonb[])                   AS errors,
        unnest(@finalized_at_do_update::boolean[]) AS finalized_at_do_update,
        unnest(@finalized_at::timestamptz[])       AS finalized_at,
        unnest(@metadata_do_merge::boolean[])      AS metadata_do_merge,
        unnest(@metadata_updates::jsonb[])         AS metadata_updates,
        unnest(@scheduled_at_do_update::boolean[]) AS scheduled_at_do_update,
        unnest(@scheduled_at::timestamptz[])       AS scheduled_at,
        -- To avoid requiring pgx users to register the OID of the river_job_state[]
        -- type, we cast the array to text[] and then to river_job_state.
        unnest(@state::text[])::/* TEMPLATE: schema */river_job_state AS state
),
updated AS (
    UPDATE /* TEMPLATE: schema */river_job
    SET
        attempt = CASE
            WHEN river_job.state = 'running'
                 AND NOT (job_input.state IN ('retryable','scheduled') AND river_job.metadata ? 'cancel_attempted_at')
                 AND job_input.attempt_do_update
            THEN job_input.attempt
            ELSE river_job.attempt
        END,
        errors = CASE
            WHEN river_job.state = 'running'
                 AND job_input.errors_do_update
            THEN array_append(river_job.errors, job_input.errors)
            ELSE river_job.errors
        END,
        finalized_at = CASE
            WHEN river_job.state = 'running'
                 AND (job_input.state IN ('retryable','scheduled') AND river_job.metadata ? 'cancel_attempted_at')
            THEN coalesce(sqlc.narg('now')::timestamptz, now())
            WHEN river_job.state = 'running'
                 AND job_input.finalized_at_do_update
            THEN job_input.finalized_at
            ELSE river_job.finalized_at
        END,
        metadata = CASE
            WHEN job_input.metadata_do_merge
            THEN river_job.metadata || job_input.metadata_updates
            ELSE river_job.metadata
        END,
        scheduled_at = CASE
            WHEN river_job.state = 'running'
                 AND NOT (job_input.state IN ('retryable','scheduled') AND river_job.metadata ? 'cancel_attempted_at')
                 AND job_input.scheduled_at_do_update
            THEN job_input.scheduled_at
            ELSE river_job.scheduled_at
        END,
        state = CASE
            WHEN river_job.state = 'running'
                 AND (job_input.state IN ('retryable','scheduled') AND river_job.metadata ? 'cancel_attempted_at')
            THEN 'cancelled'::/* TEMPLATE: schema */river_job_state
            WHEN river_job.state = 'running'
            THEN job_input.state
            ELSE river_job.state
        END
    FROM job_input
    WHERE river_job.id = job_input.id
      AND (river_job.state = 'running' OR job_input.metadata_do_merge)
    RETURNING river_job.*
)
SELECT river_job.*
FROM /* TEMPLATE: schema */river_job
JOIN job_input ON river_job.id = job_input.id
WHERE NOT EXISTS (
    SELECT 1
    FROM updated
    WHERE updated.id = river_job.id
)
UNION ALL
SELECT *
FROM updated
ORDER BY id;

-- name: JobUpdate :one
WITH locked_job AS (
    SELECT id
    FROM /* TEMPLATE: schema */river_job
    WHERE river_job.id = @id
    FOR UPDATE
)
UPDATE /* TEMPLATE: schema */river_job
SET
    metadata = CASE WHEN @metadata_do_merge::boolean THEN metadata || @metadata::jsonb ELSE metadata END
FROM
    locked_job
WHERE river_job.id = locked_job.id
RETURNING river_job.*;

-- A generalized update for any property on a job. This brings in a large number
-- of parameters and therefore may be more suitable for testing than production.
-- name: JobUpdateFull :one
UPDATE /* TEMPLATE: schema */river_job
SET
    attempt = CASE WHEN @attempt_do_update::boolean THEN @attempt ELSE attempt END,
    attempted_at = CASE WHEN @attempted_at_do_update::boolean THEN @attempted_at ELSE attempted_at END,
    attempted_by = CASE WHEN @attempted_by_do_update::boolean THEN @attempted_by ELSE attempted_by END,
    errors = CASE WHEN @errors_do_update::boolean THEN @errors::jsonb[] ELSE errors END,
    finalized_at = CASE WHEN @finalized_at_do_update::boolean THEN @finalized_at ELSE finalized_at END,
    max_attempts = CASE WHEN @max_attempts_do_update::boolean THEN @max_attempts ELSE max_attempts END,
    metadata = CASE WHEN @metadata_do_update::boolean THEN @metadata::jsonb ELSE metadata END,
    state = CASE WHEN @state_do_update::boolean THEN @state::/* TEMPLATE: schema */river_job_state ELSE state END
WHERE id = @id
RETURNING *;

-- name: JobGetWorkflowTasks :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'river:workflow_id' = @workflow_id::text
  AND (
    cardinality(@task_names::text[]) = 0
    OR metadata->>'river:workflow_task' = ANY(@task_names::text[])
  )
ORDER BY id;

-- Lists non-terminal workflow tasks whose recorded deadline has passed. Each
-- driver uses its own JSON/timestamp dialect (Postgres: ::timestamptz cast;
-- SQLite: julianday()). Used by the workflow scheduler's cancelExpiredWorkflows
-- pass.
-- name: JobGetWorkflowDeadlineExpired :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE state IN ('available','pending','retryable','running','scheduled')
  AND metadata ? 'river:workflow_deadline_at'
  AND (metadata->>'river:workflow_deadline_at')::timestamptz < @now::timestamptz
ORDER BY id
LIMIT @max::int;

-- Returns pending tasks that carry the river:workflow_wait metadata key.
-- Used by the workflow scheduler's evaluateWaits pass (dialect-correct alternative
-- to the raw `metadata ? 'key'` Postgres-only operator). Cursor pagination via
-- @after_id allows callers to page through all pending wait tasks without
-- re-fetching the same low-id rows each tick.
-- name: JobGetWorkflowWaitTasks :many
SELECT *
FROM /* TEMPLATE: schema */river_job
WHERE state = 'pending'::/* TEMPLATE: schema */river_job_state
  AND metadata ? 'river:workflow_wait'
  AND id > @after_id::bigint
ORDER BY id
LIMIT @max::int;

-- name: JobCancelWorkflow :many
WITH locked AS (
  SELECT id, queue, state
  FROM /* TEMPLATE: schema */river_job
  WHERE metadata->>'river:workflow_id' = @workflow_id::text
    AND finalized_at IS NULL
  FOR UPDATE
),
notifications AS MATERIALIZED (
  SELECT pg_notify(
    concat(coalesce(sqlc.narg('schema')::text, current_schema()), '.', @control_topic::text),
    json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text
  ) AS notified
  FROM locked
  WHERE state = 'running'
)
UPDATE /* TEMPLATE: schema */river_job
SET
  -- Leave running tasks running so their executor can cancel cleanly via
  -- the worker's context. The cancel_attempted_at metadata key tells the
  -- rescuer not to rescue them.
  state = CASE WHEN river_job.state = 'running' THEN river_job.state ELSE 'cancelled' END,
  finalized_at = CASE WHEN river_job.state = 'running' THEN river_job.finalized_at ELSE @now::timestamptz END,
  metadata = jsonb_set(
    jsonb_set(metadata, '{cancel_attempted_at}'::text[], @cancel_attempted_at::jsonb, true),
    '{river:workflow_cancel_reason}'::text[],
    to_jsonb(@reason::text),
    true
  )
FROM locked
WHERE river_job.id = locked.id
  -- Force notifications CTE to materialize so pg_notify runs.
  AND (SELECT count(*) FROM notifications) >= 0
RETURNING river_job.*;

-- name: JobUpdateWorkflowReady :many
WITH candidates AS (
  SELECT id, metadata, scheduled_at
  FROM /* TEMPLATE: schema */river_job
  WHERE state = 'pending'
    AND metadata ? 'river:workflow_id'
    AND NOT (metadata ? 'river:workflow_wait')
  ORDER BY id
  FOR UPDATE SKIP LOCKED
  LIMIT @max::int
),
dep_states AS (
  SELECT
    c.id AS candidate_id,
    sib.state AS dep_state,
    sib.metadata->>'river:workflow_task' AS dep_task
  FROM candidates c
  LEFT JOIN /* TEMPLATE: schema */river_job sib
    ON sib.metadata->>'river:workflow_id' = c.metadata->>'river:workflow_id'
   AND sib.metadata->>'river:workflow_task' IN (
         SELECT jsonb_array_elements_text(c.metadata->'river:workflow_deps')
       )
),
resolved AS (
  SELECT
    c.id,
    c.scheduled_at,
    c.metadata,
    bool_and(
      d.dep_state = 'completed'
      OR (d.dep_state = 'cancelled' AND COALESCE((c.metadata->>'river:workflow_ignore_cancelled_deps')::bool, false))
      OR (d.dep_state = 'discarded' AND COALESCE((c.metadata->>'river:workflow_ignore_discarded_deps')::bool, false))
    ) FILTER (WHERE d.dep_state IS NOT NULL) AS all_done,
    bool_or(
      d.dep_state = 'cancelled' AND NOT COALESCE((c.metadata->>'river:workflow_ignore_cancelled_deps')::bool, false)
    ) FILTER (WHERE d.dep_state IS NOT NULL) AS fail_cancelled,
    bool_or(
      d.dep_state = 'discarded' AND NOT COALESCE((c.metadata->>'river:workflow_ignore_discarded_deps')::bool, false)
    ) FILTER (WHERE d.dep_state IS NOT NULL) AS fail_discarded,
    count(d.dep_state) AS dep_rows_found,
    COALESCE((SELECT count(*) FROM jsonb_array_elements_text(c.metadata->'river:workflow_deps')), 0) AS dep_rows_declared
  FROM candidates c
  LEFT JOIN dep_states d ON d.candidate_id = c.id
  GROUP BY c.id, c.scheduled_at, c.metadata
),
classified AS (
  SELECT
    id,
    CASE
      WHEN fail_cancelled OR fail_discarded THEN 'cancelled'
      WHEN dep_rows_found < dep_rows_declared
        AND NOT COALESCE((metadata->>'river:workflow_ignore_deleted_deps')::bool, false) THEN 'cancelled'
      WHEN COALESCE(all_done, true) AND dep_rows_found >= dep_rows_declared
        AND scheduled_at > @now::timestamptz THEN 'scheduled'
      WHEN COALESCE(all_done, true) AND dep_rows_found >= dep_rows_declared THEN 'available'
      WHEN COALESCE(all_done, true)
        AND dep_rows_found < dep_rows_declared
        AND COALESCE((metadata->>'river:workflow_ignore_deleted_deps')::bool, false)
        AND scheduled_at > @now::timestamptz THEN 'scheduled'
      WHEN COALESCE(all_done, true)
        AND dep_rows_found < dep_rows_declared
        AND COALESCE((metadata->>'river:workflow_ignore_deleted_deps')::bool, false) THEN 'available'
      ELSE 'pending'
    END AS new_state
  FROM resolved
)
UPDATE /* TEMPLATE: schema */river_job j
SET state        = c.new_state::/* TEMPLATE: schema */river_job_state,
    finalized_at = CASE WHEN c.new_state = 'cancelled' THEN @now::timestamptz ELSE j.finalized_at END
FROM classified c
WHERE j.id = c.id
  AND c.new_state <> 'pending'
RETURNING j.*;

-- name: JobRetryWorkflow :many
UPDATE /* TEMPLATE: schema */river_job
SET state = CASE
        WHEN jsonb_array_length(coalesce(metadata->'river:workflow_deps', '[]'::jsonb)) > 0
        THEN 'pending'::/* TEMPLATE: schema */river_job_state
        ELSE 'available'::/* TEMPLATE: schema */river_job_state
    END,
    finalized_at = NULL,
    attempt = 0,
    attempted_at = NULL,
    attempted_by = NULL,
    errors = CASE WHEN @reset_history::bool THEN ARRAY[]::jsonb[] ELSE errors END,
    metadata = (metadata - 'cancel_attempted_at') - 'river:workflow_cancel_reason'
WHERE metadata->>'river:workflow_id' = @workflow_id::text
  -- Cast state to text to avoid needing the OID of the river_job_state[] enum
  -- array type registered (mirroring the pattern in JobSetStateIfRunningMany).
  AND state::text = ANY(@target_states::text[])
RETURNING *;

-- Promotes or cancels a single pending workflow wait task.
-- promote: state → scheduled (if scheduled_at > now) else available; sets river:workflow_wait_resolved_at.
-- cancel:  state → cancelled, finalized_at = now; sets river:workflow_wait_failed_reason.
-- No-op (returns 0 rows) if the row is not in state 'pending'.
-- name: JobApplyWorkflowWait :one
UPDATE /* TEMPLATE: schema */river_job
SET
  state = CASE
    WHEN @outcome::text = 'cancel' THEN 'cancelled'::/* TEMPLATE: schema */river_job_state
    WHEN scheduled_at > @now::timestamptz THEN 'scheduled'::/* TEMPLATE: schema */river_job_state
    ELSE 'available'::/* TEMPLATE: schema */river_job_state
  END,
  finalized_at = CASE WHEN @outcome::text = 'cancel' THEN @now::timestamptz ELSE finalized_at END,
  metadata = CASE
    WHEN @outcome::text = 'cancel'
      THEN jsonb_set(metadata, '{river:workflow_wait_failed_reason}'::text[], to_jsonb('dependency failed'::text), true)
    ELSE jsonb_set(metadata, '{river:workflow_wait_resolved_at}'::text[], to_jsonb(@now::timestamptz), true)
  END
WHERE id = @id::bigint
  AND state = 'pending'::/* TEMPLATE: schema */river_job_state
RETURNING *;