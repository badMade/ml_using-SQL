# Machine Learning Metrics SQL Toolkit

## Project Overview
This repository centralises vendor-specific SQL scripts that provision a unified `ml_metrics` table across Oracle, Amazon Redshift, Google BigQuery, Microsoft SQL Server, and Vertica. The table captures pipeline lineage, model metadata, and scalar or structured evaluation metrics so that downstream analytics tools can compare model performance consistently. Use these scripts to bootstrap observability for machine learning workloads without rewriting DDL for each data platform.

## Repository Structure
```
.
├── README.md
└── sql/
    ├── bigquery/
    │   └── create_metrics_table.sql
    ├── oracle/
    │   └── create_metrics_table.sql
    ├── redshift/
    │   └── create_metrics_table.sql
    ├── sqlserver/
    │   └── create_metrics_table.sql
    └── vertica/
        └── create_metrics_table.sql
```
Each script creates a vendor-tuned `ml_metrics` table. The folder names mirror the database technology to keep automation workflows predictable.

## Environment Prerequisites
| Platform | Client CLI / Driver | Network & Authentication | Configuration Variables |
|----------|---------------------|---------------------------|-------------------------|
| Oracle | [SQL*Plus](https://www.oracle.com/database/technologies/appdev/sqlplus.html) or `sqlcl`; Oracle Instant Client with `basic` + `sqlplus` packages | Allow TCP/1521 (or configured service port) to the target service; user with `CREATE TABLE` privileges | `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN` (EZCONNECT string) |
| Amazon Redshift | `psql` 14+ with Redshift ODBC/JDBC driver for automation | Allow TCP/5439 (or cluster port); IAM user/role with `CREATE TABLE` and `ALTER` on target schema | `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_HOST`, `REDSHIFT_PORT`, `REDSHIFT_DATABASE`, `REDSHIFT_SCHEMA` |
| Google BigQuery | `bq` CLI (Cloud SDK) with an activated service account; optional `gcloud` | Enable BigQuery API; service account with `roles/bigquery.dataOwner` on the dataset | `GOOGLE_APPLICATION_CREDENTIALS`, `BQ_PROJECT_ID`, `BQ_DATASET` |
| Microsoft SQL Server | `sqlcmd` (v18+) or Azure Data Studio; ODBC Driver 18 | Allow TCP/1433; SQL or Azure AD principal with `db_ddladmin` role | `MSSQL_USER`, `MSSQL_PASSWORD`, `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DATABASE` |
| Vertica | `vsql` CLI with Vertica client libraries | Allow TCP/5433; database user with `CREATE TABLE` privilege | `VERTICA_USER`, `VERTICA_PASSWORD`, `VERTICA_HOST`, `VERTICA_PORT`, `VERTICA_DATABASE` |

> **Note:** Store credentials securely (e.g., secrets manager or environment injection). Never commit secrets to version control.

## Step-by-Step Execution Instructions
1. **Clone and review scripts**
   ```bash
   git clone <repository-url>
   cd ml_using-SQL
   ls sql
   ```
2. **Set environment variables** according to your target platform (see table above). Consider using a `.env` file managed by your orchestrator instead of exporting secrets locally.
3. **Test connectivity** with the vendor CLI before running DDL. Example for Oracle:
   ```bash
   sqlplus "${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DSN}" <<<'SELECT 1 FROM dual;'
   ```
4. **Execute the create script**:
   - **Oracle**
     ```bash
     sqlplus "${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DSN}" @sql/oracle/create_metrics_table.sql
     ```
   - **Amazon Redshift**
     ```bash
     PGPASSWORD="${REDSHIFT_PASSWORD}" \
     psql \
       --host "${REDSHIFT_HOST}" \
       --port "${REDSHIFT_PORT:-5439}" \
       --username "${REDSHIFT_USER}" \
       --dbname "${REDSHIFT_DATABASE}" \
       --command "SET search_path TO ${REDSHIFT_SCHEMA};" \
       --file sql/redshift/create_metrics_table.sql
     ```
   - **Google BigQuery**
     ```bash
     bq query --use_legacy_sql=false \
       "$(sed -e "s/\${project_id}/${BQ_PROJECT_ID}/g" \
               -e "s/\${dataset}/${BQ_DATASET}/g" sql/bigquery/create_metrics_table.sql)"
     ```
   - **Microsoft SQL Server**
     ```bash
     sqlcmd \
       -S "${MSSQL_HOST},${MSSQL_PORT:-1433}" \
       -d "${MSSQL_DATABASE}" \
       -U "${MSSQL_USER}" \
       -P "${MSSQL_PASSWORD}" \
       -i sql/sqlserver/create_metrics_table.sql
     ```
   - **Vertica**
     ```bash
     vsql \
       -h "${VERTICA_HOST}" \
       -p "${VERTICA_PORT:-5433}" \
       -d "${VERTICA_DATABASE}" \
       -U "${VERTICA_USER}" \
       -w "${VERTICA_PASSWORD}" \
       -f sql/vertica/create_metrics_table.sql
     ```
5. **Verify deployment** by querying the metadata table (see usage examples).

### Scheduling Guidance
- **Cron / systemd timers**: Wrap the CLI invocation in a script that runs on deployment windows to keep schemas synchronised.
- **Airflow**: Use platform-specific operators (`OracleOperator`, `RedshiftSQLOperator`, `BigQueryInsertJobOperator`, `MsSqlOperator`, `VerticaOperator`) to run the DDL on deployment DAGs.
- **dbt or Liquibase**: Reference the SQL files as seeds/migrations so they can run during regular release pipelines.
- Always include idempotency checks (present in scripts via `IF NOT EXISTS` or by primary key constraints) when re-running on schedules.

## Usage Examples
After creating the table, populate metrics via inserts tailored to each platform.

- **Oracle**
  ```sql
  INSERT INTO ml_metrics (
      pipeline_id, run_id, metric_name, metric_value, metric_type,
      recorded_at, model_version, environment_label, tags, metadata_json
  ) VALUES (
      'churn-training', '2024-05-01T00:00Z', 'roc_auc', 0.942, 'scalar',
      SYSTIMESTAMP, 'v45', 'production', 'accuracy,model-a', '{"fold": 1}'
  );
  ```
- **Redshift**
  ```sql
  INSERT INTO ml_metrics VALUES (
      'reco-pipeline', '2024-05-01T00:00Z', 'hit_rate', 0.771, 'scalar',
      CURRENT_TIMESTAMP, 'v12', 'staging', ARRAY['rank-10'], '{"top_k": 10}'
  );
  ```
- **BigQuery**
  ```sql
  INSERT INTO `my-project.ml_observability.ml_metrics`
  VALUES (
      'fraud-detection', '2024-05-01T00:00Z', 'precision', 0.913, 'scalar',
      CURRENT_TIMESTAMP(), 'v9', 'production', ['financial'], JSON '{"threshold": 0.75}'
  );
  ```
- **SQL Server**
  ```sql
  INSERT INTO dbo.ml_metrics (
      pipeline_id, run_id, metric_name, metric_value, metric_type,
      recorded_at, model_version, environment_label, tags, metadata_json
  ) VALUES (
      'nlp-classifier', '2024-05-01T00:00Z', 'f1_score', 0.887, 'scalar',
      SYSDATETIMEOFFSET(), 'v3', 'uat', 'text,bert', '{"lang": "en"}'
  );
  ```
- **Vertica**
  ```sql
  INSERT INTO ml_metrics (
      pipeline_id, run_id, metric_name, metric_value, metric_type,
      recorded_at, model_version, environment_label, tags, metadata_json
  ) VALUES (
      'vision-eval', '2024-05-01T00:00Z', 'top_1_accuracy', 0.975, 'scalar',
      CURRENT_TIMESTAMP, 'v2', 'lab', ARRAY['vision','baseline'], '{"dataset": "imagenet"}'
  );
  ```

## Metrics Table Schema
| Column | Purpose | Oracle | Redshift | BigQuery | SQL Server | Vertica |
|--------|---------|--------|----------|----------|------------|---------|
| `pipeline_id` | Logical name of the ML workflow | `VARCHAR2(128)` | `VARCHAR(128)` | `STRING` | `NVARCHAR(128)` | `VARCHAR(128)` |
| `run_id` | Unique execution identifier (timestamp or UUID) | `VARCHAR2(128)` | `VARCHAR(128)` | `STRING` | `NVARCHAR(128)` | `VARCHAR(128)` |
| `metric_name` | Human-readable metric identifier | `VARCHAR2(255)` | `VARCHAR(255)` | `STRING` | `NVARCHAR(255)` | `VARCHAR(255)` |
| `metric_value` | Numeric scalar metric result | `NUMBER` | `DOUBLE PRECISION` | `FLOAT64` | `FLOAT` | `FLOAT` |
| `metric_type` | Helps distinguish scalar vs structured metrics | `VARCHAR2(64)` | `VARCHAR(64)` | `STRING` | `NVARCHAR(64)` | `VARCHAR(64)` |
| `recorded_at` | Timestamp when the metric was logged | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` | `TIMESTAMP` | `DATETIME2(7)` | `TIMESTAMPTZ` |
| `model_version` | Version tag for model artefact | `VARCHAR2(128)` | `VARCHAR(128)` | `STRING` | `NVARCHAR(128)` | `VARCHAR(128)` |
| `environment_label` | Deployment environment tag | `VARCHAR2(64)` | `VARCHAR(64)` | `STRING` | `NVARCHAR(64)` | `VARCHAR(64)` |
| `tags` | Semi-structured labels or arrays | `CLOB` | `SUPER` | `ARRAY<STRING>` | `NVARCHAR(MAX)` | `ARRAY[VARCHAR(256)]` |
| `metadata_json` | Additional structured payload | `CLOB` | `SUPER` | `JSON` | `NVARCHAR(MAX)` | `LONG VARCHAR` |

All scripts enforce a composite primary key on `(pipeline_id, run_id, metric_name)` to prevent duplicate metric ingestion. Indexing or clustering strategies are tuned to each platform (e.g., Vertica segmentation, Redshift dist/sort keys, SQL Server nonclustered index).

## Troubleshooting
- **Authentication failures**: Confirm the environment variables hold valid credentials and the account has `CREATE TABLE` rights. For managed services, double-check network security groups/firewall rules.
- **Object already exists**: Oracle lacks `IF NOT EXISTS`; reruns may raise `ORA-00955`. Drop the table manually or wrap the script inside a PL/SQL block that checks `USER_TABLES` before creation.
- **Redshift SUPER column errors**: Ensure the cluster has the [SUPER data type enabled](https://docs.aws.amazon.com/redshift/latest/dg/super-overview.html) and uses RA3 or newer node types.
- **BigQuery JSON support**: Legacy datasets may lack the JSON data type; consider using `STRING` with `PARSE_JSON` as a fallback and update the script accordingly.
- **SQL Server collation mismatches**: Align `NVARCHAR` columns with the database collation, or append `COLLATE` clauses when necessary.
- **Vertica array support**: Enable complex types via `SELECT set_preference('EnableComplexTypes','1');` before running the script on older versions.

## Contribution Guidelines
1. **Create feature branches** and keep pull requests focused on a single platform when possible.
2. **Validate syntax** with the target database locally or via containerised environments before submitting changes.
3. **Update or add SQL scripts** in the `sql/<platform>/` directory and ensure the README schema table stays in sync.
4. **Document changes** by expanding the troubleshooting section if a new platform nuance is discovered.
5. **Run linting/static checks** (e.g., `sqlfluff` or platform CLI `EXPLAIN` runs) when modifying DDL.
6. **Do not commit secrets**; use placeholder environment variable names as shown above.

By following these steps, contributors ensure the SQL scripts remain portable and ready for automation across heterogeneous data warehouses.
