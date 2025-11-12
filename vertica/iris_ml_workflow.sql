-- Vertica implementation mirroring the Oracle iris_ml_workflow pipeline.
-- Steps: load the IRIS dataset, assign folds, train Vertica in-database models,
-- evaluate using COMPUTE_CLASSIFICATION_METRICS, persist predictions, and log
-- metrics/hyper-parameters.
--
-- Non-ANSI notes:
--   * COPY WITH PARSER, CREATE MODEL, PREDICT, and COMPUTE_CLASSIFICATION_METRICS
--     are Vertica-specific extensions to ANSI SQL.
--   * The procedure uses Vertica's PL/pgSQL dialect for control flow.

-- 1. Schema and staging -------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS analytics;
SET SEARCH_PATH TO analytics;

DROP TABLE IF EXISTS iris_raw;
CREATE TABLE iris_raw (
    sepal_length FLOAT,
    sepal_width  FLOAT,
    petal_length FLOAT,
    petal_width  FLOAT,
    species      VARCHAR(32)
);

-- Load from a centralized S3 location for better portability across deployments.
-- Replace with your S3 bucket path and configure S3 credentials as needed.
COPY iris_raw
    FROM 's3://redshift-downloads/iris/iris.csv'
    PARSER FCSVPARSER()
    SKIP 1
    DELIMITER ',';

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

-- 2. Metrics, registry, prediction tables ------------------------------------------
CREATE TABLE IF NOT EXISTS iris_run_registry (
    run_id        VARCHAR(64) PRIMARY KEY,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_family  VARCHAR(64),
    notes         VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS iris_metrics (
    platform        VARCHAR(32),
    run_id          VARCHAR(64),
    model_name      VARCHAR(128),
    fold_number     INT,
    metric_name     VARCHAR(64),
    metric_value    FLOAT,
    hyperparameters VARCHAR(4000),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS iris_predictions (
    run_id            VARCHAR(64),
    model_name        VARCHAR(128),
    fold_number       INT,
    sepal_length      FLOAT,
    sepal_width       FLOAT,
    petal_length      FLOAT,
    petal_width       FLOAT,
    predicted_label   VARCHAR(32),
    predicted_prob    FLOAT,
    actual_label      VARCHAR(32),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. K-fold procedure --------------------------------------------------------------
DROP PROCEDURE IF EXISTS run_iris_kfold(INT);
CREATE OR REPLACE PROCEDURE run_iris_kfold(k INT)
AS $$
DECLARE
    fold INT := 1;
    run_id VARCHAR(64);
    model_name VARCHAR(128);
    -- Hyperparameter variables (single source of truth)
    hp_algorithm VARCHAR(64) := 'logistic_regression';
    hp_regularization VARCHAR(16) := 'l2';
    hp_lambda FLOAT := 0.1;
    hp_max_iterations INT := 100;
    hyper_json VARCHAR(4000);
BEGIN
    IF k < 2 THEN
        RAISE EXCEPTION 'k-fold must be at least 2';
    END IF;

    -- Construct JSON from hyperparameter variables
    hyper_json := '{"algorithm":"' || hp_algorithm || '",' ||
                  '"max_iterations":' || hp_max_iterations || ',' ||
                  '"regularization":"' || hp_regularization || '",' ||
                  '"lambda":' || hp_lambda || '}';

    run_id := TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') || LPAD(FLOOR(RANDOM() * 1000)::VARCHAR, 3, '0');
    INSERT INTO iris_run_registry(run_id, model_family, notes)
    VALUES (run_id, 'vertica_logistic_regression', 'Automated k-fold training run via Vertica in-database ML');

    WHILE fold <= k LOOP
        model_name := 'iris_lr_model_fold_' || fold;

        EXECUTE 'DROP MODEL IF EXISTS ' || model_name;

        EXECUTE 'CREATE MODEL ' || model_name || '
            USING LogisticRegression
            WITH PARAMETERS (regularization = ''' || hp_regularization || ''', lambda = ' || hp_lambda || ', max_iterations = ' || hp_max_iterations || ')
            AS
            SELECT species, sepal_length, sepal_width, petal_length, petal_width
            FROM iris_folds
            WHERE fold_id <> ' || fold;

        EXECUTE 'CREATE OR REPLACE LOCAL TEMP TABLE iris_predictions_holdout ON COMMIT PRESERVE ROWS AS
            SELECT
                sepal_length,
                sepal_width,
                petal_length,
                petal_width,
                species AS actual_label,
                PREDICT(' || model_name || ' USING PARAMETERS exclude_columns = ''species'', type = ''probability'', return_details = ''true'') AS prediction
            FROM iris_folds
            WHERE fold_id = ' || fold || ';';

        EXECUTE 'ALTER TABLE iris_predictions_holdout ADD COLUMN predicted_label VARCHAR(32);';
        EXECUTE 'ALTER TABLE iris_predictions_holdout ADD COLUMN predicted_prob FLOAT;';
        EXECUTE 'UPDATE iris_predictions_holdout
                 SET predicted_label = (prediction).predicted_value,
                     predicted_prob  = (prediction).probability;';

        EXECUTE 'INSERT INTO iris_predictions (run_id, model_name, fold_number, sepal_length, sepal_width, petal_length, petal_width, predicted_label, predicted_prob, actual_label)
                 SELECT ''' || run_id || ''', ''' || model_name || ''', ' || fold || ',
                        sepal_length, sepal_width, petal_length, petal_width,
                        predicted_label, predicted_prob, actual_label
                 FROM iris_predictions_holdout;';

        EXECUTE 'INSERT INTO iris_metrics (platform, run_id, model_name, fold_number, metric_name, metric_value, hyperparameters)
                 SELECT ''vertica'', ''' || run_id || ''', ''' || model_name || ''', ' || fold || ', metric_name, metric_value, ''' || hyper_json || '''
                 FROM COMPUTE_CLASSIFICATION_METRICS(
                     ON (SELECT actual_label, predicted_label, predicted_prob FROM iris_predictions_holdout)
                     USING observed_column = ''actual_label'', predicted_column = ''predicted_label'', probability_column = ''predicted_prob''
                 );';

        fold := fold + 1;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;

-- 4. Execute the pipeline -----------------------------------------------------------
CALL run_iris_kfold(5);
