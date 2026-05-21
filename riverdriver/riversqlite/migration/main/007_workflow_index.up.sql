CREATE INDEX IF NOT EXISTS /* TEMPLATE: schema */river_job_workflow_id_idx
ON /* TEMPLATE: schema */river_job (json_extract(metadata, '$."river:workflow_id"'), state)
WHERE json_extract(metadata, '$."river:workflow_id"') IS NOT NULL;
