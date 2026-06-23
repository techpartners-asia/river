CREATE TABLE river_workflow_signal (
    id bigserial PRIMARY KEY,
    workflow_id text NOT NULL,
    signal_key text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key text,
    source text,
    created_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
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
    @payload,
    sqlc.narg('idempotency_key'),
    sqlc.narg('source'),
    @now::timestamptz
)
ON CONFLICT (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *;

-- name: WorkflowSignalGetByIdempotency :one
SELECT *
FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND idempotency_key = @idempotency_key;

-- name: WorkflowSignalList :many
SELECT *
FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND (sqlc.narg('signal_key')::text IS NULL OR signal_key = sqlc.narg('signal_key'))
ORDER BY created_at, id
LIMIT @max::int;

-- name: WorkflowSignalListNewest :many
SELECT *
FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND (sqlc.narg('signal_key')::text IS NULL OR signal_key = sqlc.narg('signal_key'))
ORDER BY created_at DESC, id DESC
LIMIT @max::int;
