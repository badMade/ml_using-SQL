-- Creates the metrics table for Oracle Database deployments.
CREATE TABLE ml_metrics (
    pipeline_id        VARCHAR2(128)   NOT NULL,
    run_id             VARCHAR2(128)   NOT NULL,
    metric_name        VARCHAR2(255)   NOT NULL,
    metric_value       NUMBER,
    metric_type        VARCHAR2(64)    DEFAULT 'scalar',
    recorded_at        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP,
    model_version      VARCHAR2(128),
    environment_label  VARCHAR2(64),
    tags               CLOB,
    metadata_json      CLOB,
    CONSTRAINT pk_ml_metrics PRIMARY KEY (pipeline_id, run_id, metric_name)
);

CREATE INDEX idx_ml_metrics_recorded_at ON ml_metrics (recorded_at);
