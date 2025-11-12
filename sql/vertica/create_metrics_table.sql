-- Creates the metrics table for Vertica deployments.
CREATE TABLE IF NOT EXISTS ml_metrics (
    pipeline_id        VARCHAR(128)    NOT NULL,
    run_id             VARCHAR(128)    NOT NULL,
    metric_name        VARCHAR(255)    NOT NULL,
    metric_value       FLOAT,
    metric_type        VARCHAR(64)     DEFAULT 'scalar',
    recorded_at        TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP,
    model_version      VARCHAR(128),
    environment_label  VARCHAR(64),
    tags               ARRAY[VARCHAR(256)],
    metadata_json      LONG VARCHAR,
    PRIMARY KEY (pipeline_id, run_id, metric_name)
)
SEGMENTED BY HASH(pipeline_id) ALL NODES
PARTITION BY EXTRACT(YEAR FROM recorded_at);
