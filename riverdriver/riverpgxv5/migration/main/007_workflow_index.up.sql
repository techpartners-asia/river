CREATE INDEX IF NOT EXISTS river_job_workflow_id_idx
ON /* TEMPLATE: schema */river_job ((metadata->>'river:workflow_id'), state)
WHERE metadata ? 'river:workflow_id';
