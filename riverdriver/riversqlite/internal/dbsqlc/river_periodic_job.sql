CREATE TABLE river_periodic_job (
    id text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    next_run_at timestamp NOT NULL
);

-- name: PeriodicJobGetAll :many
SELECT *
FROM /* TEMPLATE: schema */river_periodic_job
ORDER BY id;

-- name: PeriodicJobUpsert :one
INSERT INTO /* TEMPLATE: schema */river_periodic_job (
    id,
    next_run_at,
    updated_at
) VALUES (
    @id,
    cast(@next_run_at AS text),
    cast(@updated_at AS text)
)
ON CONFLICT (id) DO UPDATE
SET
    next_run_at = EXCLUDED.next_run_at,
    updated_at  = EXCLUDED.updated_at
RETURNING *;

-- name: PeriodicJobKeepAlive :exec
UPDATE /* TEMPLATE: schema */river_periodic_job
SET updated_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
WHERE id IN (SELECT value FROM json_each(cast(@ids_json AS blob)));

-- name: PeriodicJobReap :many
DELETE FROM /* TEMPLATE: schema */river_periodic_job
WHERE id NOT IN (SELECT value FROM json_each(cast(@ids_json AS blob)))
  AND updated_at < cast(@stale_horizon AS text)
RETURNING *;
