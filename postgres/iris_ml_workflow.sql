-- PostgreSQL implementation mirroring the Oracle iris_ml_workflow pipeline.
-- Steps: load the IRIS dataset, assign folds, train using k-Nearest Neighbors
-- classification (pure SQL implementation), evaluate predictions, persist
-- predictions, and log metrics/hyper-parameters.
--
-- Non-ANSI notes:
--   * Uses PL/pgSQL for procedural control flow
--   * Uses PostgreSQL-specific array and JSONB types
--   * k-NN implemented in pure SQL for portability (no extensions required)
--   * For advanced ML, consider installing Apache MADlib extension

-- 1. Schema and staging -------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;

DROP TABLE IF EXISTS iris_raw CASCADE;
CREATE TABLE iris_raw (
    id           SERIAL PRIMARY KEY,
    sepal_length DOUBLE PRECISION,
    sepal_width  DOUBLE PRECISION,
    petal_length DOUBLE PRECISION,
    petal_width  DOUBLE PRECISION,
    species      VARCHAR(32)
);

-- Load Iris dataset (150 samples)
INSERT INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES
(5.1, 3.5, 1.4, 0.2, 'setosa'), (4.9, 3.0, 1.4, 0.2, 'setosa'), (4.7, 3.2, 1.3, 0.2, 'setosa'),
(4.6, 3.1, 1.5, 0.2, 'setosa'), (5.0, 3.6, 1.4, 0.2, 'setosa'), (5.4, 3.9, 1.7, 0.4, 'setosa'),
(4.6, 3.4, 1.4, 0.3, 'setosa'), (5.0, 3.4, 1.5, 0.2, 'setosa'), (4.4, 2.9, 1.4, 0.2, 'setosa'),
(4.9, 3.1, 1.5, 0.1, 'setosa'), (5.4, 3.7, 1.5, 0.2, 'setosa'), (4.8, 3.4, 1.6, 0.2, 'setosa'),
(4.8, 3.0, 1.4, 0.1, 'setosa'), (4.3, 3.0, 1.1, 0.1, 'setosa'), (5.8, 4.0, 1.2, 0.2, 'setosa'),
(5.7, 4.4, 1.5, 0.4, 'setosa'), (5.4, 3.9, 1.3, 0.4, 'setosa'), (5.1, 3.5, 1.4, 0.3, 'setosa'),
(5.7, 3.8, 1.7, 0.3, 'setosa'), (5.1, 3.8, 1.5, 0.3, 'setosa'), (5.4, 3.4, 1.7, 0.2, 'setosa'),
(5.1, 3.7, 1.5, 0.4, 'setosa'), (4.6, 3.6, 1.0, 0.2, 'setosa'), (5.1, 3.3, 1.7, 0.5, 'setosa'),
(4.8, 3.4, 1.9, 0.2, 'setosa'), (5.0, 3.0, 1.6, 0.2, 'setosa'), (5.0, 3.4, 1.6, 0.4, 'setosa'),
(5.2, 3.5, 1.5, 0.2, 'setosa'), (5.2, 3.4, 1.4, 0.2, 'setosa'), (4.7, 3.2, 1.6, 0.2, 'setosa'),
(4.8, 3.1, 1.6, 0.2, 'setosa'), (5.4, 3.4, 1.5, 0.4, 'setosa'), (5.2, 4.1, 1.5, 0.1, 'setosa'),
(5.5, 4.2, 1.4, 0.2, 'setosa'), (4.9, 3.1, 1.5, 0.2, 'setosa'), (5.0, 3.2, 1.2, 0.2, 'setosa'),
(5.5, 3.5, 1.3, 0.2, 'setosa'), (4.9, 3.6, 1.4, 0.1, 'setosa'), (4.4, 3.0, 1.3, 0.2, 'setosa'),
(5.1, 3.4, 1.5, 0.2, 'setosa'), (5.0, 3.5, 1.3, 0.3, 'setosa'), (4.5, 2.3, 1.3, 0.3, 'setosa'),
(4.4, 3.2, 1.3, 0.2, 'setosa'), (5.0, 3.5, 1.6, 0.6, 'setosa'), (5.1, 3.8, 1.9, 0.4, 'setosa'),
(4.8, 3.0, 1.4, 0.3, 'setosa'), (5.1, 3.8, 1.6, 0.2, 'setosa'), (4.6, 3.2, 1.4, 0.2, 'setosa'),
(5.3, 3.7, 1.5, 0.2, 'setosa'), (5.0, 3.3, 1.4, 0.2, 'setosa'),
(7.0, 3.2, 4.7, 1.4, 'versicolor'), (6.4, 3.2, 4.5, 1.5, 'versicolor'), (6.9, 3.1, 4.9, 1.5, 'versicolor'),
(5.5, 2.3, 4.0, 1.3, 'versicolor'), (6.5, 2.8, 4.6, 1.5, 'versicolor'), (5.7, 2.8, 4.5, 1.3, 'versicolor'),
(6.3, 3.3, 4.7, 1.6, 'versicolor'), (4.9, 2.4, 3.3, 1.0, 'versicolor'), (6.6, 2.9, 4.6, 1.3, 'versicolor'),
(5.2, 2.7, 3.9, 1.4, 'versicolor'), (5.0, 2.0, 3.5, 1.0, 'versicolor'), (5.9, 3.0, 4.2, 1.5, 'versicolor'),
(6.0, 2.2, 4.0, 1.0, 'versicolor'), (6.1, 2.9, 4.7, 1.4, 'versicolor'), (5.6, 2.9, 3.6, 1.3, 'versicolor'),
(6.7, 3.1, 4.4, 1.4, 'versicolor'), (5.6, 3.0, 4.5, 1.5, 'versicolor'), (5.8, 2.7, 4.1, 1.0, 'versicolor'),
(6.2, 2.2, 4.5, 1.5, 'versicolor'), (5.6, 2.5, 3.9, 1.1, 'versicolor'), (5.9, 3.2, 4.8, 1.8, 'versicolor'),
(6.1, 2.8, 4.0, 1.3, 'versicolor'), (6.3, 2.5, 4.9, 1.5, 'versicolor'), (6.1, 2.8, 4.7, 1.2, 'versicolor'),
(6.4, 2.9, 4.3, 1.3, 'versicolor'), (6.6, 3.0, 4.4, 1.4, 'versicolor'), (6.8, 2.8, 4.8, 1.4, 'versicolor'),
(6.7, 3.0, 5.0, 1.7, 'versicolor'), (6.0, 2.9, 4.5, 1.5, 'versicolor'), (5.7, 2.6, 3.5, 1.0, 'versicolor'),
(5.5, 2.4, 3.8, 1.1, 'versicolor'), (5.5, 2.4, 3.7, 1.0, 'versicolor'), (5.8, 2.7, 3.9, 1.2, 'versicolor'),
(6.0, 2.7, 5.1, 1.6, 'versicolor'), (5.4, 3.0, 4.5, 1.5, 'versicolor'), (6.0, 3.4, 4.5, 1.6, 'versicolor'),
(6.7, 3.1, 4.7, 1.5, 'versicolor'), (6.3, 2.3, 4.4, 1.3, 'versicolor'), (5.6, 3.0, 4.1, 1.3, 'versicolor'),
(5.5, 2.5, 4.0, 1.3, 'versicolor'), (5.5, 2.6, 4.4, 1.2, 'versicolor'), (6.1, 3.0, 4.6, 1.4, 'versicolor'),
(5.8, 2.6, 4.0, 1.2, 'versicolor'), (5.0, 2.3, 3.3, 1.0, 'versicolor'), (5.6, 2.7, 4.2, 1.3, 'versicolor'),
(5.7, 3.0, 4.2, 1.2, 'versicolor'), (5.7, 2.9, 4.2, 1.3, 'versicolor'), (6.2, 2.9, 4.3, 1.3, 'versicolor'),
(5.1, 2.5, 3.0, 1.1, 'versicolor'), (5.7, 2.8, 4.1, 1.3, 'versicolor'),
(6.3, 3.3, 6.0, 2.5, 'virginica'), (5.8, 2.7, 5.1, 1.9, 'virginica'), (7.1, 3.0, 5.9, 2.1, 'virginica'),
(6.3, 2.9, 5.6, 1.8, 'virginica'), (6.5, 3.0, 5.8, 2.2, 'virginica'), (7.6, 3.0, 6.6, 2.1, 'virginica'),
(4.9, 2.5, 4.5, 1.7, 'virginica'), (7.3, 2.9, 6.3, 1.8, 'virginica'), (6.7, 2.5, 5.8, 1.8, 'virginica'),
(7.2, 3.6, 6.1, 2.5, 'virginica'), (6.5, 3.2, 5.1, 2.0, 'virginica'), (6.4, 2.7, 5.3, 1.9, 'virginica'),
(6.8, 3.0, 5.5, 2.1, 'virginica'), (5.7, 2.5, 5.0, 2.0, 'virginica'), (5.8, 2.8, 5.1, 2.4, 'virginica'),
(6.4, 3.2, 5.3, 2.3, 'virginica'), (6.5, 3.0, 5.5, 1.8, 'virginica'), (7.7, 3.8, 6.7, 2.2, 'virginica'),
(7.7, 2.6, 6.9, 2.3, 'virginica'), (6.0, 2.2, 5.0, 1.5, 'virginica'), (6.9, 3.2, 5.7, 2.3, 'virginica'),
(5.6, 2.8, 4.9, 2.0, 'virginica'), (7.7, 2.8, 6.7, 2.0, 'virginica'), (6.3, 2.7, 4.9, 1.8, 'virginica'),
(6.7, 3.3, 5.7, 2.1, 'virginica'), (7.2, 3.2, 6.0, 1.8, 'virginica'), (6.2, 2.8, 4.8, 1.8, 'virginica'),
(6.1, 3.0, 4.9, 1.8, 'virginica'), (6.4, 2.8, 5.6, 2.1, 'virginica'), (7.2, 3.0, 5.8, 1.6, 'virginica'),
(7.4, 2.8, 6.1, 1.9, 'virginica'), (7.9, 3.8, 6.4, 2.0, 'virginica'), (6.4, 2.8, 5.6, 2.2, 'virginica'),
(6.3, 2.8, 5.1, 1.5, 'virginica'), (6.1, 2.6, 5.6, 1.4, 'virginica'), (7.7, 3.0, 6.1, 2.3, 'virginica'),
(6.3, 3.4, 5.6, 2.4, 'virginica'), (6.4, 3.1, 5.5, 1.8, 'virginica'), (6.0, 3.0, 4.8, 1.8, 'virginica'),
(6.9, 3.1, 5.4, 2.1, 'virginica'), (6.7, 3.1, 5.6, 2.4, 'virginica'), (6.9, 3.1, 5.1, 2.3, 'virginica'),
(5.8, 2.7, 5.1, 1.9, 'virginica'), (6.8, 3.2, 5.9, 2.3, 'virginica'), (6.7, 3.3, 5.7, 2.5, 'virginica'),
(6.7, 3.0, 5.2, 2.3, 'virginica'), (6.3, 2.5, 5.0, 1.9, 'virginica'), (6.5, 3.0, 5.2, 2.0, 'virginica'),
(6.2, 3.4, 5.4, 2.3, 'virginica'), (5.9, 3.0, 5.1, 1.8, 'virginica');

DROP TABLE IF EXISTS iris_folds CASCADE;
CREATE TABLE iris_folds AS
SELECT
    id,
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    species,
    NTILE(5) OVER (ORDER BY random()) AS fold_id
FROM iris_raw;

-- 2. Metrics, registry, prediction tables ------------------------------------------
CREATE TABLE IF NOT EXISTS iris_run_registry (
    run_id        VARCHAR(64) PRIMARY KEY,
    created_at    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    model_family  VARCHAR(64),
    notes         VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS iris_metrics (
    platform        VARCHAR(32),
    run_id          VARCHAR(64),
    model_name      VARCHAR(128),
    fold_number     INT,
    metric_name     VARCHAR(64),
    metric_value    DOUBLE PRECISION,
    hyperparameters JSONB,
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS iris_predictions (
    run_id            VARCHAR(64),
    model_name        VARCHAR(128),
    fold_number       INT,
    sepal_length      DOUBLE PRECISION,
    sepal_width       DOUBLE PRECISION,
    petal_length      DOUBLE PRECISION,
    petal_width       DOUBLE PRECISION,
    predicted_label   VARCHAR(32),
    predicted_prob    DOUBLE PRECISION,
    actual_label      VARCHAR(32),
    created_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3. k-NN prediction function -------------------------------------------------------
-- Pure SQL k-Nearest Neighbors implementation using Euclidean distance
CREATE OR REPLACE FUNCTION knn_predict(
    p_sepal_length DOUBLE PRECISION,
    p_sepal_width DOUBLE PRECISION,
    p_petal_length DOUBLE PRECISION,
    p_petal_width DOUBLE PRECISION,
    p_k INT,
    p_exclude_fold INT
)
RETURNS TABLE (predicted_species VARCHAR(32), confidence DOUBLE PRECISION) AS $$
BEGIN
    RETURN QUERY
    WITH distances AS (
        SELECT
            f.species,
            SQRT(
                POWER(f.sepal_length - p_sepal_length, 2) +
                POWER(f.sepal_width - p_sepal_width, 2) +
                POWER(f.petal_length - p_petal_length, 2) +
                POWER(f.petal_width - p_petal_width, 2)
            ) AS distance
        FROM iris_folds f
        WHERE f.fold_id <> p_exclude_fold
        ORDER BY distance
        LIMIT p_k
    ),
    votes AS (
        SELECT
            species,
            COUNT(*) AS vote_count
        FROM distances
        GROUP BY species
        ORDER BY vote_count DESC
        LIMIT 1
    )
    SELECT
        v.species::VARCHAR(32),
        v.vote_count::DOUBLE PRECISION / p_k
    FROM votes v;
END;
$$ LANGUAGE plpgsql;

-- 4. K-fold procedure --------------------------------------------------------------
-- Security: Uses parameterized queries throughout to prevent SQL injection.
DROP PROCEDURE IF EXISTS run_iris_kfold(INT, INT);
CREATE OR REPLACE PROCEDURE run_iris_kfold(num_folds INT, k_neighbors INT DEFAULT 5)
LANGUAGE plpgsql
AS $$
DECLARE
    fold INT := 1;
    v_run_id VARCHAR(64);
    v_model_name VARCHAR(128);
    v_hyper_json JSONB;
    v_correct INT;
    v_total INT;
    v_accuracy DOUBLE PRECISION;
    rec RECORD;
    v_pred_species VARCHAR(32);
    v_pred_conf DOUBLE PRECISION;
BEGIN
    IF num_folds < 2 THEN
        RAISE EXCEPTION 'k-fold must be at least 2';
    END IF;

    -- Construct hyperparameters JSON
    v_hyper_json := jsonb_build_object(
        'algorithm', 'k_nearest_neighbors',
        'k', k_neighbors,
        'distance_metric', 'euclidean'
    );

    -- Generate unique run ID
    v_run_id := TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') ||
                LPAD(FLOOR(random() * 1000)::TEXT, 3, '0');

    INSERT INTO iris_run_registry(run_id, model_family, notes)
    VALUES (v_run_id, 'knn_classifier', 'Automated k-fold training run via PostgreSQL k-NN');

    WHILE fold <= num_folds LOOP
        v_model_name := 'iris_knn_model_fold_' || fold;

        -- Make predictions for holdout fold
        FOR rec IN
            SELECT id, sepal_length, sepal_width, petal_length, petal_width, species
            FROM iris_folds
            WHERE fold_id = fold
        LOOP
            SELECT predicted_species, confidence
            INTO v_pred_species, v_pred_conf
            FROM knn_predict(
                rec.sepal_length,
                rec.sepal_width,
                rec.petal_length,
                rec.petal_width,
                k_neighbors,
                fold
            );

            INSERT INTO iris_predictions (
                run_id, model_name, fold_number,
                sepal_length, sepal_width, petal_length, petal_width,
                predicted_label, predicted_prob, actual_label
            ) VALUES (
                v_run_id, v_model_name, fold,
                rec.sepal_length, rec.sepal_width, rec.petal_length, rec.petal_width,
                v_pred_species, v_pred_conf, rec.species
            );
        END LOOP;

        -- Calculate metrics for this fold
        SELECT
            COUNT(*) FILTER (WHERE predicted_label = actual_label),
            COUNT(*)
        INTO v_correct, v_total
        FROM iris_predictions
        WHERE run_id = v_run_id AND fold_number = fold;

        v_accuracy := v_correct::DOUBLE PRECISION / NULLIF(v_total, 0);

        -- Log accuracy metric
        INSERT INTO iris_metrics (
            platform, run_id, model_name, fold_number,
            metric_name, metric_value, hyperparameters
        ) VALUES (
            'postgres', v_run_id, v_model_name, fold,
            'accuracy', v_accuracy, v_hyper_json
        );

        -- Log total predictions
        INSERT INTO iris_metrics (
            platform, run_id, model_name, fold_number,
            metric_name, metric_value, hyperparameters
        ) VALUES (
            'postgres', v_run_id, v_model_name, fold,
            'total_predictions', v_total, v_hyper_json
        );

        fold := fold + 1;
    END LOOP;

    -- Log overall cross-validation accuracy
    SELECT AVG(metric_value)
    INTO v_accuracy
    FROM iris_metrics
    WHERE run_id = v_run_id AND metric_name = 'accuracy';

    INSERT INTO iris_metrics (
        platform, run_id, model_name, fold_number,
        metric_name, metric_value, hyperparameters
    ) VALUES (
        'postgres', v_run_id, 'iris_knn_model_cv', 0,
        'cv_mean_accuracy', v_accuracy, v_hyper_json
    );

    RAISE NOTICE 'Run % completed. Mean CV accuracy: %', v_run_id, v_accuracy;
END;
$$;

-- 5. Execute the pipeline -----------------------------------------------------------
CALL run_iris_kfold(5, 5);

-- 6. View results -------------------------------------------------------------------
-- SELECT * FROM iris_metrics ORDER BY created_at DESC LIMIT 20;
-- SELECT * FROM iris_predictions ORDER BY created_at DESC LIMIT 20;
