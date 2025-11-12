-- Creates the metrics table for Google BigQuery deployments.
CREATE TABLE IF NOT EXISTS `${project_id}.${dataset}.ml_metrics` (
    pipeline_id        STRING      NOT NULL,
    run_id             STRING      NOT NULL,
    metric_name        STRING      NOT NULL,
    metric_value       FLOAT64,
    metric_type        STRING      DEFAULT 'scalar',
    recorded_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP(),
    model_version      STRING,
    environment_label  STRING,
    tags               ARRAY<STRING>,
    metadata_json      JSON,
    PRIMARY KEY (pipeline_id, run_id, metric_name) NOT ENFORCED
);
