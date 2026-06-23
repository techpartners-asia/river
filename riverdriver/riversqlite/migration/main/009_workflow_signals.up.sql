CREATE TABLE /* TEMPLATE: schema */river_workflow_signal (
    id integer PRIMARY KEY AUTOINCREMENT,
    workflow_id text NOT NULL,
    signal_key text NOT NULL,
    payload text NOT NULL DEFAULT '{}',
    idempotency_key text,
    source text,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at timestamp,
    CONSTRAINT river_workflow_signal_workflow_id_length CHECK (length(workflow_id) > 0 AND length(workflow_id) < 128),
    CONSTRAINT river_workflow_signal_signal_key_length CHECK (length(signal_key) > 0 AND length(signal_key) < 128)
);
CREATE UNIQUE INDEX river_workflow_signal_idempotency ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX river_workflow_signal_lookup ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, signal_key, created_at);
