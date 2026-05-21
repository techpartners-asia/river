CREATE TABLE /* TEMPLATE: schema */river_periodic_job (
    id text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    next_run_at timestamp NOT NULL,
    CONSTRAINT river_periodic_job_id_length CHECK (length(id) > 0 AND length(id) < 128)
);
