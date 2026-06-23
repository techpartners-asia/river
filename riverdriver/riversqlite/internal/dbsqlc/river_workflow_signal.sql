CREATE TABLE river_workflow_signal (
    id integer PRIMARY KEY AUTOINCREMENT,
    workflow_id text NOT NULL,
    signal_key text NOT NULL,
    payload text NOT NULL DEFAULT '{}',
    idempotency_key text,
    source text,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at timestamp
);

-- name: WorkflowSignalEmit :one
INSERT INTO /* TEMPLATE: schema */river_workflow_signal (
    workflow_id,
    signal_key,
    payload,
    idempotency_key,
    source,
    created_at
) VALUES (
    @workflow_id,
    @signal_key,
    cast(@payload AS text),
    cast(sqlc.narg('idempotency_key') AS text),
    cast(sqlc.narg('source') AS text),
    cast(@now AS text)
)
ON CONFLICT (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *;

-- name: WorkflowSignalGetByIdempotency :one
SELECT *
FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND idempotency_key = cast(@idempotency_key AS text);

-- name: WorkflowSignalList :many
SELECT *
FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND (sqlc.narg('signal_key') IS NULL OR signal_key = cast(sqlc.narg('signal_key') AS text))
ORDER BY created_at, id
LIMIT cast(@max AS integer);

-- name: WorkflowSignalListNewest :many
SELECT *
FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND (sqlc.narg('signal_key') IS NULL OR signal_key = cast(sqlc.narg('signal_key') AS text))
ORDER BY created_at DESC, id DESC
LIMIT cast(@max AS integer);
