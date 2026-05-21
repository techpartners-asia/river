CREATE TABLE river_periodic_job (
    id text PRIMARY KEY NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    next_run_at timestamptz NOT NULL
);

-- name: PeriodicJobGetAll :many
SELECT *
FROM /* TEMPLATE: schema */river_periodic_job
ORDER BY id;

-- name: PeriodicJobUpsertMany :many
INSERT INTO /* TEMPLATE: schema */river_periodic_job (
    id,
    next_run_at,
    updated_at
)
SELECT
    unnest(@id::text[]),
    unnest(@next_run_at::timestamptz[]),
    unnest(@updated_at::timestamptz[])
ON CONFLICT (id) DO UPDATE
SET
    next_run_at = EXCLUDED.next_run_at,
    updated_at  = EXCLUDED.updated_at
RETURNING *;

-- name: PeriodicJobKeepAliveAndReap :many
WITH touched AS (
    UPDATE /* TEMPLATE: schema */river_periodic_job
    SET updated_at = coalesce(sqlc.narg('now')::timestamptz, now())
    WHERE id = ANY(@id::text[])
    RETURNING id
),
reaped AS (
    DELETE FROM /* TEMPLATE: schema */river_periodic_job
    WHERE NOT (id = ANY(@id::text[]))
      AND updated_at < @stale_horizon::timestamptz
    RETURNING *
)
SELECT id, created_at, updated_at, next_run_at FROM reaped
WHERE (SELECT count(*) FROM touched) >= 0;
