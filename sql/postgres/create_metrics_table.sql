-- Creates the metrics table for PostgreSQL deployments.
CREATE TABLE IF NOT EXISTS ml_metrics (
    pipeline_id        VARCHAR(128)    NOT NULL,
    run_id             VARCHAR(128)    NOT NULL,
    metric_name        VARCHAR(255)    NOT NULL,
    metric_value       DOUBLE PRECISION,
    metric_type        VARCHAR(64)     DEFAULT 'scalar',
    recorded_at        TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP,
    model_version      VARCHAR(128),
    environment_label  VARCHAR(64),
    tags               TEXT[],
    metadata_json      JSONB,
    PRIMARY KEY (pipeline_id, run_id, metric_name)
);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_ml_metrics_recorded_at
    ON ml_metrics (recorded_at);

-- GIN index for efficient JSONB queries on metadata
CREATE INDEX IF NOT EXISTS idx_ml_metrics_metadata
    ON ml_metrics USING GIN (metadata_json);

-- GIN index for efficient array queries on tags
CREATE INDEX IF NOT EXISTS idx_ml_metrics_tags
    ON ml_metrics USING GIN (tags);
