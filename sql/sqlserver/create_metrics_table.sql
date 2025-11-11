-- Creates the metrics table for Microsoft SQL Server deployments.
CREATE TABLE dbo.ml_metrics (
    pipeline_id        NVARCHAR(128)   NOT NULL,
    run_id             NVARCHAR(128)   NOT NULL,
    metric_name        NVARCHAR(255)   NOT NULL,
    metric_value       FLOAT NULL,
    metric_type        NVARCHAR(64)    CONSTRAINT df_ml_metrics_metric_type DEFAULT 'scalar',
    recorded_at        DATETIME2(7)    CONSTRAINT df_ml_metrics_recorded_at DEFAULT SYSDATETIMEOFFSET(),
    model_version      NVARCHAR(128)   NULL,
    environment_label  NVARCHAR(64)    NULL,
    tags               NVARCHAR(MAX)   NULL,
    metadata_json      NVARCHAR(MAX)   NULL,
    CONSTRAINT pk_ml_metrics PRIMARY KEY CLUSTERED (pipeline_id, run_id, metric_name)
);

CREATE NONCLUSTERED INDEX idx_ml_metrics_recorded_at ON dbo.ml_metrics (recorded_at);
