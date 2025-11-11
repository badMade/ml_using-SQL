-- Amazon Redshift implementation of the Oracle iris_ml_workflow pipeline.
-- This script mirrors the Oracle workflow steps: data preparation, k-fold training,
-- evaluation, prediction materialisation, and metric logging.
--
-- Non-ANSI notes:
--   * Redshift ML specific statements (CREATE MODEL, ML.EVALUATE, ML.PREDICT) are
--     Amazon-proprietary extensions to ANSI SQL.
--   * The COPY command expects access to the public sample dataset hosted by AWS S3.
--   * PL/pgSQL stored procedures (LANGUAGE plpgsql) are PostgreSQL-compatible and
--     required for procedural control flow such as k-fold iteration.

-- 1. Schema and base table alignment -------------------------------------------------
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;

DROP TABLE IF EXISTS iris_raw;
CREATE TABLE iris_raw (
    sepal_length REAL,
    sepal_width  REAL,
    petal_length REAL,
    petal_width  REAL,
    species      VARCHAR(32)
);

-- The Oracle workflow loads IRIS data from a staging area. Redshift mirrors that
-- behaviour by using the public AWS sample. Replace the placeholder IAM role ARN below
-- with a dedicated role that follows the principle of least privilege (read access to
-- the specific S3 bucket and path only).
COPY iris_raw
FROM 's3://redshift-downloads/iris/iris.csv'
IAM_ROLE 'arn:aws:iam::<aws-account-id>:role/<role-name-for-redshift-copy>'
FORMAT AS CSV
IGNOREHEADER 1;

-- 2. Create a reproducible fold assignment -----------------------------------------
DROP TABLE IF EXISTS iris_folds;
CREATE TABLE iris_folds AS
SELECT
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    species,
    NTILE(5) OVER (ORDER BY RANDOM()) AS fold_id
FROM iris_raw;

-- 3. Metrics & hyper-parameter logging tables --------------------------------------
-- The structure intentionally mirrors the Oracle metrics table for portability.
CREATE TABLE IF NOT EXISTS iris_run_registry (
    run_id        VARCHAR(64)   ENCODE zstd,
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    model_family  VARCHAR(64),
    notes         VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS iris_metrics (
    platform        VARCHAR(32)   ENCODE zstd,
    run_id          VARCHAR(64)   ENCODE zstd,
    model_name      VARCHAR(128)  ENCODE zstd,
    fold_number     INTEGER,
    metric_name     VARCHAR(64),
    metric_value    DOUBLE PRECISION,
    hyperparameters VARCHAR(4000),
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- Store scored rows per fold for diagnostic parity with Oracle.
CREATE TABLE IF NOT EXISTS iris_predictions (
    run_id          VARCHAR(64),
    model_name      VARCHAR(128),
    fold_number     INTEGER,
    sepal_length    REAL,
    sepal_width     REAL,
    petal_length    REAL,
    petal_width     REAL,
    predicted_label VARCHAR(32),
    predicted_prob  DOUBLE PRECISION,
    actual_label    VARCHAR(32),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Stored procedure for Redshift ML k-fold training ------------------------------
DROP PROCEDURE IF EXISTS run_iris_kfold(integer);
CREATE OR REPLACE PROCEDURE run_iris_kfold(k INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    fold INTEGER := 1;
    run_id VARCHAR(64);
    model_identifier VARCHAR(128);
    hyper_json VARCHAR(4000) := '{"function":"random_forest","max_depth":5,"num_trees":200,"sample_ratio":1.0}';
BEGIN
    IF k < 2 THEN
        RAISE EXCEPTION 'k-fold value % must be >= 2', k;
    END IF;

    run_id := REPLACE(to_char(GETDATE(), 'YYYYMMDDHH24MISS'), ' ', '') || LPAD((RANDOM()*1000)::INT::TEXT, 3, '0');

    INSERT INTO iris_run_registry(run_id, model_family, notes)
    VALUES (run_id, 'redshift_random_forest', 'Automated k-fold training run');

    WHILE fold <= k LOOP
        model_identifier := format('iris_rf_model_fold_%s', fold);

        EXECUTE format(
            $$CREATE OR REPLACE MODEL analytics.%I
              FROM (
                SELECT sepal_length, sepal_width, petal_length, petal_width, species
                FROM analytics.iris_folds
                WHERE fold_id <> %L
              )
              TARGET species
              FUNCTION random_forest
              SETTINGS (
                max_depth = 5,
                num_trees = 200,
                sample_ratio = 1.0
              );$$,
            model_identifier,
            fold
        );

        -- Snapshot evaluation metrics once per fold.
        EXECUTE format(
            $$CREATE TEMP TABLE IF NOT EXISTS analytics.tmp_iris_eval AS
                SELECT *
                FROM ML.EVALUATE(MODEL analytics.%I,
                     (SELECT sepal_length, sepal_width, petal_length, petal_width, species
                      FROM analytics.iris_folds WHERE fold_id = %L));$$,
            model_identifier,
            fold
        );

        EXECUTE format(
            $$INSERT INTO analytics.iris_metrics (platform, run_id, model_name, fold_number, metric_name, metric_value, hyperparameters)
              SELECT 'redshift', %L, %L, %s, metric_name, metric_value, %L
              FROM (
                  SELECT 'accuracy'  AS metric_name, accuracy  AS metric_value FROM analytics.tmp_iris_eval
                  UNION ALL
                  SELECT 'precision' AS metric_name, precision AS metric_value FROM analytics.tmp_iris_eval
                  UNION ALL
                  SELECT 'recall'    AS metric_name, recall    AS metric_value FROM analytics.tmp_iris_eval
                  UNION ALL
                  SELECT 'f1'        AS metric_name, f1        AS metric_value FROM analytics.tmp_iris_eval
                  UNION ALL
                  SELECT 'log_loss'  AS metric_name, log_loss  AS metric_value FROM analytics.tmp_iris_eval
              ) metrics;$$,
            run_id,
            model_identifier,
            fold,
            hyper_json
        );

        -- Persist predictions for downstream diagnostics. Redshift ML exposes
        -- predicted probability as predicted_label_prob for the chosen class.
        EXECUTE format(
            $$INSERT INTO analytics.iris_predictions (run_id, model_name, fold_number, sepal_length, sepal_width, petal_length, petal_width, predicted_label, predicted_prob, actual_label)
              SELECT
                  %L,
                  %L,
                  %s,
                  p.sepal_length,
                  p.sepal_width,
                  p.petal_length,
                  p.petal_width,
                  p.predicted_label,
                  p.predicted_label_prob,
                  actual.species,
                  CURRENT_TIMESTAMP
              FROM ML.PREDICT(MODEL analytics.%I,
                   (SELECT sepal_length, sepal_width, petal_length, petal_width FROM analytics.iris_folds WHERE fold_id = %L)
              ) AS p
              JOIN analytics.iris_folds AS actual
                ON p.sepal_length = actual.sepal_length
               AND p.sepal_width  = actual.sepal_width
               AND p.petal_length = actual.petal_length
               AND p.petal_width  = actual.petal_width
              WHERE actual.fold_id = %L;$$,
            run_id,
            model_identifier,
            fold,
            model_identifier,
            fold,
            fold
        );

        EXECUTE 'DROP TABLE IF EXISTS analytics.tmp_iris_eval';

        fold := fold + 1;
    END LOOP;
END;
$$;

-- 5. Execute the pipeline -----------------------------------------------------------
CALL run_iris_kfold(5);
