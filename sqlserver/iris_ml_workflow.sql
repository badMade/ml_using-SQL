/*
SQL Server implementation mirroring the Oracle iris_ml_workflow pipeline.
The workflow uses SQL Server Machine Learning Services (Python) to train an
ONNX classification model, persists the artefact for scoring with the native
PREDICT function, evaluates metrics in T-SQL, and stores results following the
Oracle metrics schema.

Non-ANSI notes:
  * BULK INSERT, sp_execute_external_script, and PREDICT are SQL Server
    extensions.
  * The script assumes Machine Learning Services (Python) and the skl2onnx
    package are installed in the SQL Server environment.
*/

-- 1. Schema and source table setup ---------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'analytics')
    EXEC('CREATE SCHEMA analytics');
GO

IF OBJECT_ID(N'analytics.iris_raw', N'U') IS NOT NULL
    DROP TABLE analytics.iris_raw;
GO

CREATE TABLE analytics.iris_raw (
    sepal_length FLOAT NOT NULL,
    sepal_width  FLOAT NOT NULL,
    petal_length FLOAT NOT NULL,
    petal_width  FLOAT NOT NULL,
    species      NVARCHAR(32) NOT NULL
);
GO

-- Provide the data file path as a SQLCMD variable so the script can run in
-- different environments without editing the T-SQL itself. Examples:
--   Windows local path:      :setvar IrisCsvPath "C:\\data\\iris.csv"
--   Linux path:              :setvar IrisCsvPath "/var/opt/sqlserver/data/iris.csv"
--   External data source:    Create EXTERNAL DATA SOURCE IrisExternal WITH (...)
--                            then set :setvar IrisCsvPath "iris.csv" and
--                            specify DATA_SOURCE = 'IrisExternal' below.
-- See https://learn.microsoft.com/sql/t-sql/functions/openrowset-transact-sql
-- for supported options.

-- IMPORTANT: Update this path to point to your local iris.csv file.
:setvar IrisCsvPath "C:\\path\\to\\iris.csv"

-- Using OPENROWSET with a configurable source path
INSERT INTO analytics.iris_raw (sepal_length, sepal_width, petal_length, petal_width, species)
SELECT
    CAST([sepal_length] AS FLOAT),
    CAST([sepal_width] AS FLOAT),
    CAST([petal_length] AS FLOAT),
    CAST([petal_width] AS FLOAT),
    CAST([species] AS NVARCHAR(32))
FROM OPENROWSET(
    BULK N'$(IrisCsvPath)',
    FORMAT = 'CSV',
    FIRSTROW = 2
) AS iris_data([sepal_length], [sepal_width], [petal_length], [petal_width], [species]);
GO

IF OBJECT_ID(N'analytics.iris_folds', N'U') IS NOT NULL
    DROP TABLE analytics.iris_folds;
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

-- 3. K-fold stored procedure --------------------------------------------------------
IF OBJECT_ID(N'analytics.run_iris_kfold', N'P') IS NOT NULL
    DROP PROCEDURE analytics.run_iris_kfold;
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
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LogisticRegression
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

X = InputDataSet[["sepal_length", "sepal_width", "petal_length", "petal_width"]]
y = InputDataSet["species"].copy()
encoder = LabelEncoder()
y_encoded = encoder.fit_transform(y)
model = LogisticRegression(max_iter=200, solver="lbfgs", multi_class="auto")
model.fit(X, y_encoded)
initial_type = [("float_input", FloatTensorType([None, 4]))]
onnx_model = convert_sklearn(model, initial_types=initial_type, target_opset=12)
OutputDataSet = pd.DataFrame({"model_onnx": [onnx_model.SerializeToString()]})
',
            @input_data_1 = N'SELECT sepal_length, sepal_width, petal_length, petal_width, species FROM #iris_train',
            @output_data_1_name = N'OutputDataSet';

        MERGE analytics.iris_models AS target
        USING (SELECT @run_id AS run_id, @fold AS fold_number, @model_name AS model_name, model_onnx, @hyperparameters AS hyperparameters FROM @model) AS source
        ON target.run_id = source.run_id AND target.fold_number = source.fold_number
        WHEN MATCHED THEN
            UPDATE SET model_name = source.model_name, model_onnx = source.model_onnx, hyperparameters = source.hyperparameters, created_at = SYSUTCDATETIME()
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

        WITH class_metrics AS (
            SELECT
                c.class_label,
                SUM(CASE WHEN s.actual_label = c.class_label AND s.predicted_label = c.class_label THEN 1 ELSE 0 END) AS tp,
                SUM(CASE WHEN s.actual_label <> c.class_label AND s.predicted_label = c.class_label THEN 1 ELSE 0 END) AS fp,
                SUM(CASE WHEN s.actual_label = c.class_label AND s.predicted_label <> c.class_label THEN 1 ELSE 0 END) AS fn
            FROM (VALUES (N'setosa'), (N'versicolor'), (N'virginica')) AS c(class_label)
            LEFT JOIN #scored AS s ON 1 = 1
            GROUP BY c.class_label
        ),
        aggregated AS (
            SELECT
                AVG(CASE WHEN tp + fp = 0 THEN 0 ELSE CAST(tp AS FLOAT) / (tp + fp) END) AS precision_macro,
                AVG(CASE WHEN tp + fn = 0 THEN 0 ELSE CAST(tp AS FLOAT) / (tp + fn) END) AS recall_macro,
                AVG(CASE WHEN (2 * tp + fp + fn) = 0 THEN 0 ELSE (2.0 * tp) / (2 * tp + fp + fn) END) AS f1_macro
            FROM class_metrics
        ),
        accuracy_metric AS (
            SELECT AVG(CASE WHEN predicted_label = actual_label THEN 1.0 ELSE 0.0 END) AS accuracy
            FROM #scored
        ),
        logloss_metric AS (
            SELECT AVG(
                CASE actual_label
                    WHEN N'setosa' THEN -LOG(NULLIF(probability_setosa, 0))
                    WHEN N'versicolor' THEN -LOG(NULLIF(probability_versicolor, 0))
                    WHEN N'virginica' THEN -LOG(NULLIF(probability_virginica, 0))
                END
            ) AS log_loss
            FROM #scored
        )
        INSERT INTO analytics.iris_metrics (platform, run_id, model_name, fold_number, metric_name, metric_value, hyperparameters)
        SELECT N'sqlserver', @run_id, @model_name, @fold, metric_name, metric_value, @hyperparameters
        FROM (
            SELECT N'accuracy' AS metric_name, accuracy AS metric_value FROM accuracy_metric
            UNION ALL
            SELECT N'precision' AS metric_name, precision_macro FROM aggregated
            UNION ALL
            SELECT N'recall'    AS metric_name, recall_macro FROM aggregated
            UNION ALL
            SELECT N'f1'        AS metric_name, f1_macro FROM aggregated
            UNION ALL
            SELECT N'log_loss'  AS metric_name, log_loss FROM logloss_metric
        ) AS metrics;

        SET @fold += 1;
    END;
END;
GO

-- 4. Execute the pipeline -----------------------------------------------------------
EXEC analytics.run_iris_kfold @k = 5;
GO
