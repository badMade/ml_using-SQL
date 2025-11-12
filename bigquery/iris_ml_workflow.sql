-- BigQuery implementation mirroring the Oracle iris_ml_workflow pipeline.
-- Steps: ingest IRIS sample, assign folds, train BigQuery ML models, evaluate,
-- log metrics, and materialise predictions.
--
-- Non-ANSI notes:
--   * CREATE MODEL, ML.EVALUATE, and ML.PREDICT are BigQuery ML extensions.
--   * BigQuery scripting constructs (DECLARE, WHILE, EXECUTE IMMEDIATE) extend
--     ANSI SQL for control flow.

-- 1. Dataset preparation -------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `iris_ml`;

CREATE OR REPLACE TABLE `iris_ml.iris_raw` AS
SELECT
  sepal_length,
  sepal_width,
  petal_length,
  petal_width,
  CAST(CASE species WHEN 'Iris-setosa' THEN 'setosa'
                    WHEN 'Iris-versicolor' THEN 'versicolor'
                    ELSE 'virginica' END AS STRING) AS species
FROM `bigquery-public-data.ml_datasets.iris`;

CREATE OR REPLACE TABLE `iris_ml.iris_folds` AS
SELECT *, NTILE(5) OVER (ORDER BY RAND()) AS fold_id
FROM `iris_ml.iris_raw`;

-- 2. Metrics registry ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `iris_ml.iris_run_registry` (
  run_id       STRING,
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  model_family STRING,
  notes        STRING
);

CREATE TABLE IF NOT EXISTS `iris_ml.iris_metrics` (
  platform        STRING,
  run_id          STRING,
  model_name      STRING,
  fold_number     INT64,
  metric_name     STRING,
  metric_value    FLOAT64,
  hyperparameters STRING,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `iris_ml.iris_predictions` (
  run_id          STRING,
  model_name      STRING,
  fold_number     INT64,
  sepal_length    FLOAT64,
  sepal_width     FLOAT64,
  petal_length    FLOAT64,
  petal_width     FLOAT64,
  predicted_label STRING,
  predicted_prob  FLOAT64,
  actual_label    STRING,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3. K-fold training script --------------------------------------------------------
DECLARE k INT64 DEFAULT 5;
DECLARE fold INT64 DEFAULT 1;
DECLARE run_id STRING DEFAULT FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP());
DECLARE model_type STRING DEFAULT 'logistic_reg';
DECLARE max_iterations INT64 DEFAULT 50;
DECLARE l2_reg FLOAT64 DEFAULT 0.1;
DECLARE hyperparameters STRING DEFAULT FORMAT('{"model_type":"%s","max_iterations":%d,"l2_reg":%f}', model_type, max_iterations, l2_reg);
DECLARE model_name STRING;
DECLARE eval_sql STRING;
DECLARE predict_sql STRING;

INSERT INTO `iris_ml.iris_run_registry` (run_id, model_family, notes)
VALUES (run_id, 'bigquery_logistic_reg', 'Automated k-fold training run via BigQuery ML');

WHILE fold <= k DO
  SET model_name = FORMAT('iris_lr_model_fold_%d', fold);

  EXECUTE IMMEDIATE FORMAT(
    '''CREATE OR REPLACE MODEL `iris_ml.%s`
        OPTIONS (model_type = ''%s'', input_label_cols = [''species''], max_iterations = %d, l2_reg = %f)
      AS
      SELECT sepal_length, sepal_width, petal_length, petal_width, species
      FROM `iris_ml.iris_folds`
      WHERE fold_id <> %d''',
    model_name,
    model_type,
    max_iterations,
    l2_reg,
    fold
  );

  SET eval_sql = FORMAT(
    '''INSERT INTO `iris_ml.iris_metrics` (platform, run_id, model_name, fold_number, metric_name, metric_value, hyperparameters)
         WITH evaluation AS (
           SELECT *
           FROM ML.EVALUATE(
             MODEL `iris_ml.%s`,
             (SELECT sepal_length, sepal_width, petal_length, petal_width, species
              FROM `iris_ml.iris_folds`
              WHERE fold_id = %d)
           )
         )
         SELECT ''bigquery'', @run_id, ''%s'', %d, metric_name, metric_value, @hyperparameters
         FROM evaluation, UNNEST([
           STRUCT(''accuracy'' AS metric_name, accuracy AS metric_value),
           STRUCT(''precision'', precision),
           STRUCT(''recall'', recall),
           STRUCT(''f1'', f1),
           STRUCT(''log_loss'', log_loss)
         ])''',
    model_name,
    fold,
    model_name,
    fold
  );

  EXECUTE IMMEDIATE eval_sql USING run_id AS run_id, hyperparameters AS hyperparameters;

  SET predict_sql = FORMAT(
    '''INSERT INTO `iris_ml.iris_predictions` (
          run_id, model_name, fold_number, sepal_length, sepal_width, petal_length, petal_width, 
          predicted_label, predicted_prob, actual_label, created_at
        )
         SELECT
           @run_id,
           ''%s'',
           %d,
           p.sepal_length,
           p.sepal_width,
           p.petal_length,
           p.petal_width,
           p.predicted_label,
           (SELECT prob FROM UNNEST(p.predicted_label_probs) WHERE label = p.predicted_label LIMIT 1) AS predicted_prob,
           p.species AS actual_label,
           CURRENT_TIMESTAMP()
         FROM ML.PREDICT(MODEL `iris_ml.%s`, (
             SELECT sepal_length, sepal_width, petal_length, petal_width, species
             FROM `iris_ml.iris_folds`
             WHERE fold_id = %d
         )) AS p''',
    model_name,
    fold,
    model_name,
    fold
  );

  EXECUTE IMMEDIATE predict_sql USING run_id AS run_id;

  SET fold = fold + 1;
END WHILE;
