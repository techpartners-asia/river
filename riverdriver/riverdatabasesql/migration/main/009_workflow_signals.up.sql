CREATE TABLE /* TEMPLATE: schema */river_workflow_signal (
    id bigserial PRIMARY KEY,
    workflow_id text NOT NULL,
    signal_key text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key text,
    source text,
    created_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    CONSTRAINT river_workflow_signal_workflow_id_length CHECK (char_length(workflow_id) > 0 AND char_length(workflow_id) < 128),
    CONSTRAINT river_workflow_signal_signal_key_length CHECK (char_length(signal_key) > 0 AND char_length(signal_key) < 128)
);
CREATE UNIQUE INDEX river_workflow_signal_idempotency ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX river_workflow_signal_lookup ON /* TEMPLATE: schema */river_workflow_signal (workflow_id, signal_key, created_at);
