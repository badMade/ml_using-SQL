-- SQL Server implementation mirroring the Oracle iris_ml_workflow pipeline.
-- The workflow uses SQL Server Machine Learning Services (Python) to train an
-- ONNX classification model, persists the artefact for scoring with the native
-- PREDICT function, evaluates metrics in T-SQL, and stores results following the
-- Oracle metrics schema.
--
-- Non-ANSI notes:
--   * BULK INSERT, sp_execute_external_script, and PREDICT are SQL Server
--     extensions.
--   * The script assumes Machine Learning Services (Python) and the skl2onnx
--     package are installed in the SQL Server environment.

-- 1. Schema and source table setup ---------------------------------------------------
-- Note: SQL Server requires database-level users to be created via separate scripts
-- with proper authentication (SQL or Windows). This script assumes the 'analytics'
-- schema already exists or will be created by a DBA.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'analytics')
BEGIN
    EXEC('CREATE SCHEMA analytics');
END;
GO

IF OBJECT_ID(N'analytics.iris_raw', N'U') IS NOT NULL
BEGIN
    DROP TABLE analytics.iris_raw;
END;
GO

CREATE TABLE analytics.iris_raw (
    sepal_length FLOAT NOT NULL,
    sepal_width  FLOAT NOT NULL,
    petal_length FLOAT NOT NULL,
    petal_width  FLOAT NOT NULL,
    species      NVARCHAR(32) NOT NULL
);
GO

-- Load the IRIS CSV from a configured data source.
-- For production deployments, use one of the following approaches:
--   * Azure Blob Storage: Configure an EXTERNAL DATA SOURCE and use BULK INSERT
--     or OPENROWSET with a URL like:
--     'https://<account>.blob.core.windows.net/<container>/iris.csv'
--   * Network share accessible to the SQL Server service account:
--     '\\\\server\\share\\iris.csv'
--   * HTTP/HTTPS endpoint with appropriate authentication
--
-- The example below uses a public HTTPS endpoint. Replace with your actual data
-- source. See: https://docs.microsoft.com/sql/t-sql/statements/bulk-insert-transact-sql

-- Using OPENROWSET for flexibility with HTTPS sources
INSERT INTO analytics.iris_raw (sepal_length, sepal_width, petal_length, petal_width, species)
SELECT 
    CAST([sepal_length] AS FLOAT),
    CAST([sepal_width] AS FLOAT),
    CAST([petal_length] AS FLOAT),
    CAST([petal_width] AS FLOAT),
    CAST([species] AS NVARCHAR(32))
FROM OPENROWSET(
    BULK N'https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv',
    FORMAT = 'CSV',
    FIRSTROW = 2
) WITH (
    sepal_length FLOAT,
    sepal_width FLOAT,
    petal_length FLOAT,
    petal_width FLOAT,
    species NVARCHAR(32)
) AS source_data;
GO

IF OBJECT_ID(N'analytics.iris_folds', N'U') IS NOT NULL
BEGIN
    DROP TABLE analytics.iris_folds;
END;
GO

SELECT *, NTILE(5) OVER (ORDER BY NEWID()) AS fold_id
INTO analytics.iris_folds
FROM analytics.iris_raw;
GO

-- 2. Metrics, registry, and prediction logging --------------------------------------
IF OBJECT_ID(N'analytics.iris_run_registry', N'U') IS NULL
BEGIN
    CREATE TABLE analytics.iris_run_registry (
        run_id       UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        created_at   DATETIME2(3)     NOT NULL DEFAULT SYSUTCDATETIME(),
        model_family NVARCHAR(64)     NOT NULL,
        notes        NVARCHAR(256)    NULL
    );
END;
GO

IF OBJECT_ID(N'analytics.iris_metrics', N'U') IS NULL
BEGIN
    CREATE TABLE analytics.iris_metrics (
        platform        NVARCHAR(32)  NOT NULL,
        run_id          UNIQUEIDENTIFIER NOT NULL,
        model_name      NVARCHAR(128) NOT NULL,
        fold_number     INT NOT NULL,
        metric_name     NVARCHAR(64)  NOT NULL,
        metric_value    FLOAT          NOT NULL,
        hyperparameters NVARCHAR(4000) NOT NULL,
        created_at      DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'analytics.iris_models', N'U') IS NULL
BEGIN
    CREATE TABLE analytics.iris_models (
        run_id          UNIQUEIDENTIFIER NOT NULL,
        fold_number     INT NOT NULL,
        model_name      NVARCHAR(128) NOT NULL,
        model_onnx      VARBINARY(MAX) NOT NULL,
        hyperparameters NVARCHAR(4000) NOT NULL,
        created_at      DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_iris_models PRIMARY KEY (run_id, fold_number)
    );
END;
GO

IF OBJECT_ID(N'analytics.iris_predictions', N'U') IS NULL
BEGIN
    CREATE TABLE analytics.iris_predictions (
        run_id          UNIQUEIDENTIFIER NOT NULL,
        model_name      NVARCHAR(128) NOT NULL,
        fold_number     INT NOT NULL,
        sepal_length    FLOAT NOT NULL,
        sepal_width     FLOAT NOT NULL,
        petal_length    FLOAT NOT NULL,
        petal_width     FLOAT NOT NULL,
        predicted_label NVARCHAR(32) NOT NULL,
        probability_setosa FLOAT NULL,
        probability_versicolor FLOAT NULL,
        probability_virginica FLOAT NULL,
        actual_label    NVARCHAR(32) NOT NULL,
        created_at      DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'analytics.run_iris_kfold', N'P') IS NOT NULL
BEGIN
    DROP PROCEDURE analytics.run_iris_kfold;
END;
GO

CREATE PROCEDURE analytics.run_iris_kfold
    @k INT = 5
AS
BEGIN
    SET NOCOUNT ON;

    IF @k < 2
    BEGIN
        THROW 50001, 'k-fold value must be at least 2.', 1;
    END;

    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @fold INT = 1;
    DECLARE @model_name NVARCHAR(128);
    DECLARE @hyperparameters NVARCHAR(4000) = N'{"algorithm":"logistic_regression","max_iter":200,"solver":"lbfgs"}';

    INSERT INTO analytics.iris_run_registry (run_id, model_family, notes)
    VALUES (@run_id, N'sqlserver_logistic_regression', N'Automated k-fold training run via SQL Server ML Services');

    WHILE @fold <= @k
    BEGIN
        SET @model_name = FORMATMESSAGE(N'iris_lr_model_fold_%d', @fold);

        IF OBJECT_ID('tempdb..#iris_train') IS NOT NULL DROP TABLE #iris_train;
        IF OBJECT_ID('tempdb..#iris_test') IS NOT NULL DROP TABLE #iris_test;

        SELECT sepal_length, sepal_width, petal_length, petal_width, species
        INTO #iris_train
        FROM analytics.iris_folds
        WHERE fold_id <> @fold;

        SELECT sepal_length, sepal_width, petal_length, petal_width, species
        INTO #iris_test
        FROM analytics.iris_folds
        WHERE fold_id = @fold;

        DECLARE @model TABLE (model_onnx VARBINARY(MAX));

        INSERT INTO @model (model_onnx)
        EXEC sp_execute_external_script
            @language = N'Python',
            @script = N'
import json
import pandas as pd
from sklearn.linear_model import LogisticRegression
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType, StringTensorType

# Parse hyperparameters from JSON
hyperparams = json.loads(hyperparameters_json)
solver = hyperparams.get("solver", "lbfgs")
max_iter = int(hyperparams.get("max_iter", 200))
multi_class = hyperparams.get("multi_class", "auto")

X = InputDataSet[["sepal_length", "sepal_width", "petal_length", "petal_width"]]
y = InputDataSet["species"]
model = LogisticRegression(max_iter=max_iter, solver=solver, multi_class=multi_class)
model.fit(X, y)
initial_type = [("float_input", FloatTensorType([None, 4]))]
onnx_model = convert_sklearn(model, initial_types=initial_type, target_opset=12)
OutputDataSet = pd.DataFrame({"model_onnx": [onnx_model.SerializeToString()]})
',
            @input_data_1 = N'SELECT sepal_length, sepal_width, petal_length, petal_width, species FROM #iris_train',
            @output_data_1_name = N'OutputDataSet',
            @params = N'@hyperparameters_json NVARCHAR(4000)',
            @hyperparameters_json = @hyperparameters;

        MERGE analytics.iris_models AS target
        USING (
            SELECT
                @run_id AS run_id,
                @fold AS fold_number,
                @model_name AS model_name,
                model_onnx,
                @hyperparameters AS hyperparameters
            FROM @model
        ) AS source
        ON target.run_id = source.run_id AND target.fold_number = source.fold_number
        WHEN MATCHED THEN
            UPDATE SET model_name = source.model_name,
                       model_onnx = source.model_onnx,
                       hyperparameters = source.hyperparameters,
                       created_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (run_id, fold_number, model_name, model_onnx, hyperparameters)
            VALUES (source.run_id, source.fold_number, source.model_name, source.model_onnx, source.hyperparameters);

        DECLARE @model_blob VARBINARY(MAX) = (SELECT model_onnx FROM @model);

        IF OBJECT_ID('tempdb..#scored') IS NOT NULL DROP TABLE #scored;

        -- Probability column names follow the pattern Probability_<label> emitted by PREDICT
        -- when the ONNX model exposes class probabilities. Adjust if the runtime decorates
        -- labels differently (for example, spaces become underscores).
        SELECT
            scored.sepal_length,
            scored.sepal_width,
            scored.petal_length,
            scored.petal_width,
            scored.PredictedLabel AS predicted_label,
            scored.[Probability_setosa] AS probability_setosa,
            scored.[Probability_versicolor] AS probability_versicolor,
            scored.[Probability_virginica] AS probability_virginica,
            test.species AS actual_label
        INTO #scored
        FROM PREDICT (MODEL = @model_blob,
                      DATA = (SELECT sepal_length, sepal_width, petal_length, petal_width FROM #iris_test)) AS scored
        INNER JOIN #iris_test AS test
            ON scored.sepal_length = test.sepal_length
           AND scored.sepal_width  = test.sepal_width
           AND scored.petal_length = test.petal_length
           AND scored.petal_width  = test.petal_width;

        INSERT INTO analytics.iris_predictions (
            run_id, model_name, fold_number,
            sepal_length, sepal_width, petal_length, petal_width,
            predicted_label, probability_setosa, probability_versicolor, probability_virginica,
            actual_label)
        SELECT
            @run_id,
            @model_name,
            @fold,
            sepal_length,
            sepal_width,
            petal_length,
            petal_width,
            predicted_label,
            probability_setosa,
            probability_versicolor,
            probability_virginica,
            actual_label
        FROM #scored;

        -- Calculate classification metrics using Python and scikit-learn
        DECLARE @metrics TABLE (metric_name NVARCHAR(64), metric_value FLOAT);

        INSERT INTO @metrics (metric_name, metric_value)
        EXEC sp_execute_external_script
            @language = N'Python',
            @script = N'
import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, log_loss

# Extract actual and predicted labels
y_true = InputDataSet["actual_label"].values
y_pred = InputDataSet["predicted_label"].values

# Extract probability columns for log-loss calculation
prob_cols = ["probability_setosa", "probability_versicolor", "probability_virginica"]
y_prob = InputDataSet[prob_cols].values

# Define label order for probability alignment
labels = ["setosa", "versicolor", "virginica"]

# Calculate metrics
accuracy = accuracy_score(y_true, y_pred)
precision = precision_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
recall = recall_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
f1 = f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
logloss = log_loss(y_true, y_prob, labels=labels)

# Create output dataframe with metrics
metrics_data = [
    ("accuracy", accuracy),
    ("precision", precision),
    ("recall", recall),
    ("f1", f1),
    ("log_loss", logloss)
]

OutputDataSet = pd.DataFrame(metrics_data, columns=["metric_name", "metric_value"])
',
            @input_data_1 = N'SELECT actual_label, predicted_label, probability_setosa, probability_versicolor, probability_virginica FROM #scored',
            @output_data_1_name = N'OutputDataSet';

        -- Insert metrics with metadata
        INSERT INTO analytics.iris_metrics (platform, run_id, model_name, fold_number, metric_name, metric_value, hyperparameters)
        SELECT N'sqlserver', @run_id, @model_name, @fold, metric_name, metric_value, @hyperparameters
        FROM @metrics;

        SET @fold += 1;
    END;
END;
GO

-- 4. Execute the pipeline -----------------------------------------------------------
EXEC analytics.run_iris_kfold @k = 5;
GO
