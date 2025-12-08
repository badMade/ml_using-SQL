# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Machine Learning Metrics SQL Toolkit - provides vendor-specific SQL scripts for creating a unified `ml_metrics` table across Oracle, Amazon Redshift, Google BigQuery, Microsoft SQL Server, and Vertica. Used to bootstrap ML observability infrastructure without rewriting DDL for each platform.

## Linting Commands

**SQL** (using sqlfluff):
```bash
sqlfluff lint sql/

## Architecture

### DDL Scripts (`sql/<platform>/`)
Each database has a `create_metrics_table.sql` that creates the `ml_metrics` table with platform-specific optimizations:
- Composite primary key: `(pipeline_id, run_id, metric_name)`
- Platform-tuned data types (e.g., Redshift SUPER, BigQuery JSON, Vertica ARRAY)
- Idempotent via `IF NOT EXISTS` where supported

### ML Workflow Scripts (`<platform>/iris_ml_workflow.sql`)
End-to-end ML workflow demos using Iris dataset:
- K-fold cross-validation implementation
- Model training and evaluation
- Metrics logging to registry tables

### Schema Columns
`pipeline_id`, `run_id`, `metric_name`, `metric_value`, `metric_type`, `recorded_at`, `model_version`, `environment_label`, `tags`, `metadata_json`

## Platform-Specific Notes

- **Oracle**: Lacks `IF NOT EXISTS`; may raise `ORA-00955` on reruns
- **Redshift**: Requires SUPER data type enabled (RA3+ nodes)
- **BigQuery**: Uses template variables `${project_id}` and `${dataset}`
- **Vertica**: May need `SET set_preference('EnableComplexTypes','1')` for arrays
