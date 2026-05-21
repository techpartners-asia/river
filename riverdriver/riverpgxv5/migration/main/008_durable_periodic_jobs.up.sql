CREATE TABLE /* TEMPLATE: schema */river_periodic_job (
    id text PRIMARY KEY NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    next_run_at timestamptz NOT NULL,
    CONSTRAINT river_periodic_job_id_length CHECK (char_length(id) > 0 AND char_length(id) < 128)
);
