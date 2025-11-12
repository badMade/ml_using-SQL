-- Creates the metrics table for Amazon Redshift deployments.
CREATE TABLE IF NOT EXISTS ml_metrics (
    pipeline_id        VARCHAR(128)    ENCODE zstd NOT NULL,
    run_id             VARCHAR(128)    ENCODE zstd NOT NULL,
    metric_name        VARCHAR(255)    ENCODE zstd NOT NULL,
    metric_value       DOUBLE PRECISION,
    metric_type        VARCHAR(64)     ENCODE zstd DEFAULT 'scalar',
    recorded_at        TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP,
    model_version      VARCHAR(128)    ENCODE zstd,
    environment_label  VARCHAR(64)     ENCODE zstd,
    tags               SUPER,
    metadata_json      SUPER,
    PRIMARY KEY (pipeline_id, run_id, metric_name)
)
DISTSTYLE KEY
DISTKEY (pipeline_id)
SORTKEY (recorded_at);
