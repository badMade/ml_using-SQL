-- Oracle Machine Learning Workflow for the Iris Dataset
-- ----------------------------------------------------
-- This script creates reusable objects that demonstrate an Oracle-based
-- machine learning workflow leveraging ANSI SQL constructs wherever possible.
-- Oracle-specific statements and package calls are annotated accordingly.

-- #########################################################################
-- Section 1: Drop existing objects (Oracle-specific EXECUTE IMMEDIATE usage)
-- #########################################################################
DECLARE
    PROCEDURE safe_drop(p_sql VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE p_sql;
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE NOT IN (-942, -4043, -2289) THEN
                RAISE;
            END IF;
    END;
BEGIN
    safe_drop('DROP TABLE iris_metrics');
    safe_drop('DROP TABLE iris_retraining_schedule');
    safe_drop('DROP TABLE iris_model_lifecycle');
    safe_drop('DROP TABLE iris_model_parameters');
    safe_drop('DROP TABLE iris_model_parameter_sets');
    safe_drop('DROP TABLE iris_folds');
    safe_drop('DROP TABLE iris_raw');
    safe_drop('DROP GLOBAL TEMPORARY TABLE iris_dm_settings');
END;
/

-- ############################################################
-- Section 2: Create and populate the source Iris data (ANSI SQL)
-- ############################################################
CREATE TABLE iris_raw (
    id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sepal_length  NUMBER(4,2) NOT NULL,
    sepal_width   NUMBER(4,2) NOT NULL,
    petal_length  NUMBER(4,2) NOT NULL,
    petal_width   NUMBER(4,2) NOT NULL,
    species       VARCHAR2(20) NOT NULL
);

INSERT ALL
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.5, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.9, 3.0, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.7, 3.2, 1.3, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.6, 3.1, 1.5, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.6, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.4, 3.9, 1.7, 0.4, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.6, 3.4, 1.4, 0.3, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.4, 1.5, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.4, 2.9, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.9, 3.1, 1.5, 0.1, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.4, 3.7, 1.5, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.8, 3.4, 1.6, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.8, 3.0, 1.4, 0.1, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.3, 3.0, 1.1, 0.1, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.8, 4.0, 1.2, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 4.4, 1.5, 0.4, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.4, 3.9, 1.3, 0.4, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.5, 1.4, 0.3, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 3.8, 1.7, 0.3, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.8, 1.5, 0.3, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.4, 3.4, 1.7, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.7, 1.5, 0.4, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.6, 3.6, 1.0, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.3, 1.7, 0.5, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.8, 3.4, 1.9, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.0, 1.6, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.4, 1.6, 0.4, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.2, 3.5, 1.5, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.2, 3.4, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.7, 3.2, 1.6, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.8, 3.1, 1.6, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.4, 3.4, 1.5, 0.4, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.2, 4.1, 1.5, 0.1, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.5, 4.2, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.9, 3.1, 1.5, 0.1, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.2, 1.2, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.5, 3.5, 1.3, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.9, 3.6, 1.4, 0.1, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.4, 3.0, 1.3, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.4, 1.5, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.5, 1.3, 0.3, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.5, 2.3, 1.3, 0.3, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.4, 3.2, 1.3, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.5, 1.6, 0.6, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.8, 1.9, 0.4, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.8, 3.0, 1.4, 0.3, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 3.8, 1.6, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.6, 3.2, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.3, 3.7, 1.5, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 3.3, 1.4, 0.2, 'Iris-setosa')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.0, 3.2, 4.7, 1.4, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.4, 3.2, 4.5, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.9, 3.1, 4.9, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.5, 2.3, 4.0, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.5, 2.8, 4.6, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 2.8, 4.5, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 3.3, 4.7, 1.6, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.9, 2.4, 3.3, 1.0, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.6, 2.9, 4.6, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.2, 2.7, 3.9, 1.4, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 2.0, 3.5, 1.0, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.9, 3.0, 4.2, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.0, 2.2, 4.0, 1.0, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.1, 2.9, 4.7, 1.4, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.6, 2.9, 3.6, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 3.1, 4.4, 1.4, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.6, 3.0, 4.5, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.8, 2.7, 4.1, 1.0, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.2, 2.2, 4.5, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.6, 2.5, 3.9, 1.1, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.9, 3.2, 4.8, 1.8, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.1, 2.8, 4.0, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 2.5, 4.9, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.1, 2.8, 4.7, 1.2, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.4, 2.9, 4.3, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.6, 3.0, 4.4, 1.4, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.8, 2.8, 4.8, 1.4, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 3.0, 5.0, 1.7, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.0, 2.9, 4.5, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 2.6, 3.5, 1.0, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.5, 2.4, 3.8, 1.1, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.5, 2.4, 3.7, 1.0, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.8, 2.7, 3.9, 1.2, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.0, 2.7, 5.1, 1.6, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.4, 3.0, 4.5, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.0, 3.4, 4.5, 1.6, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 3.1, 4.7, 1.5, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 2.3, 4.4, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.6, 3.0, 4.1, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.5, 2.5, 4.0, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.5, 2.6, 4.4, 1.2, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.1, 3.0, 4.6, 1.4, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.8, 2.6, 4.0, 1.2, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.0, 2.3, 3.3, 1.0, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.6, 2.7, 4.2, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 3.0, 4.2, 1.2, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 2.9, 4.2, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.2, 2.9, 4.3, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.1, 2.5, 3.0, 1.1, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 2.8, 4.1, 1.3, 'Iris-versicolor')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 3.3, 6.0, 2.5, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.8, 2.7, 5.1, 1.9, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.1, 3.0, 5.9, 2.1, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 2.9, 5.6, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.5, 3.0, 5.8, 2.2, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.6, 3.0, 6.6, 2.1, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (4.9, 2.5, 4.5, 1.7, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.3, 2.9, 6.3, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 2.5, 5.8, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.2, 3.6, 6.1, 2.5, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.5, 3.2, 5.1, 2.0, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.4, 2.7, 5.3, 1.9, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.8, 3.0, 5.5, 2.1, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.7, 2.5, 5.0, 2.0, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.8, 2.8, 5.1, 2.4, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.4, 3.2, 5.3, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.5, 3.0, 5.5, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.7, 3.8, 6.7, 2.2, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.7, 2.6, 6.9, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.0, 2.2, 5.0, 1.5, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.9, 3.2, 5.7, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.6, 2.8, 4.9, 2.0, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.7, 2.8, 6.7, 2.0, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 2.7, 4.9, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 3.3, 5.7, 2.1, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.2, 3.2, 6.0, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.2, 2.8, 4.8, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.1, 3.0, 4.9, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.4, 2.8, 5.6, 2.1, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.2, 3.0, 5.8, 1.6, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.4, 2.8, 6.1, 1.9, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.9, 3.8, 6.4, 2.0, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.4, 2.8, 5.6, 2.2, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 2.8, 5.1, 1.5, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.1, 2.6, 5.6, 1.4, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (7.7, 3.0, 6.1, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 3.4, 5.6, 2.4, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.4, 3.1, 5.5, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.0, 3.0, 4.8, 1.8, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.9, 3.1, 5.4, 2.1, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 3.1, 5.6, 2.4, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.9, 3.1, 5.1, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.8, 2.7, 5.1, 1.9, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.8, 3.2, 5.9, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 3.3, 5.7, 2.5, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.7, 3.0, 5.2, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.3, 2.5, 5.0, 1.9, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.5, 3.0, 5.2, 2.0, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (6.2, 3.4, 5.4, 2.3, 'Iris-virginica')
    INTO iris_raw (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (5.9, 3.0, 5.1, 1.8, 'Iris-virginica')
SELECT * FROM dual;

COMMIT;

-- ########################################################
-- Section 3: Assign fold identifiers using ANSI analytics
-- ########################################################
CREATE TABLE iris_folds AS
SELECT id,
       sepal_length,
       sepal_width,
       petal_length,
       petal_width,
       species,
       NTILE(5) OVER (
           PARTITION BY species
           ORDER BY DBMS_RANDOM.VALUE
       ) AS fold_id
  FROM iris_raw;

-- #############################################################
-- Section 4: Hyper-parameter catalogs and lifecycle tables (ANSI)
-- #############################################################
CREATE TABLE iris_model_parameter_sets (
    parameter_set_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    algorithm        VARCHAR2(50) NOT NULL,
    version_tag      VARCHAR2(30) NOT NULL,
    created_on       TIMESTAMP    DEFAULT SYSTIMESTAMP
);

CREATE TABLE iris_model_parameters (
    parameter_set_id NUMBER       NOT NULL,
    parameter_name   VARCHAR2(100) NOT NULL,
    parameter_value  VARCHAR2(4000) NOT NULL,
    CONSTRAINT iris_model_parameters_fk FOREIGN KEY (parameter_set_id)
        REFERENCES iris_model_parameter_sets(parameter_set_id)
);

INSERT INTO iris_model_parameter_sets (algorithm, version_tag) VALUES ('DECISION_TREE', 'v1');
INSERT INTO iris_model_parameter_sets (algorithm, version_tag) VALUES ('DECISION_TREE', 'v2');
INSERT INTO iris_model_parameter_sets (algorithm, version_tag) VALUES ('RANDOM_FOREST', 'v1');
INSERT INTO iris_model_parameter_sets (algorithm, version_tag) VALUES ('RANDOM_FOREST', 'v2');
INSERT INTO iris_model_parameter_sets (algorithm, version_tag) VALUES ('SVM', 'v1');
INSERT INTO iris_model_parameter_sets (algorithm, version_tag) VALUES ('SVM', 'v2');

-- Decision Tree parameters (Oracle-specific setting names)
INSERT INTO iris_model_parameters (parameter_set_id, parameter_name, parameter_value)
SELECT p.parameter_set_id, v.parameter_name, v.parameter_value
  FROM iris_model_parameter_sets p
 CROSS JOIN
       ( SELECT 'ALGO_NAME' AS parameter_name, 'ALGO_DECISION_TREE' AS parameter_value FROM dual UNION ALL
         SELECT 'TREE_TERM_MAX_DEPTH', '6' FROM dual UNION ALL
         SELECT 'TREE_IMPURITY_METRIC', 'TREE_IMPURITY_GINI' FROM dual
       ) v
 WHERE p.algorithm = 'DECISION_TREE' AND p.version_tag = 'v1';

INSERT INTO iris_model_parameters (parameter_set_id, parameter_name, parameter_value)
SELECT p.parameter_set_id, v.parameter_name, v.parameter_value
  FROM iris_model_parameter_sets p
 CROSS JOIN
       ( SELECT 'ALGO_NAME' AS parameter_name, 'ALGO_DECISION_TREE' AS parameter_value FROM dual UNION ALL
         SELECT 'TREE_TERM_MAX_DEPTH', '12' FROM dual UNION ALL
         SELECT 'TREE_IMPURITY_METRIC', 'TREE_IMPURITY_ENTROPY' FROM dual
       ) v
 WHERE p.algorithm = 'DECISION_TREE' AND p.version_tag = 'v2';

-- Random Forest parameters (Oracle-specific setting names)
INSERT INTO iris_model_parameters (parameter_set_id, parameter_name, parameter_value)
SELECT p.parameter_set_id, v.parameter_name, v.parameter_value
  FROM iris_model_parameter_sets p
 CROSS JOIN
       ( SELECT 'ALGO_NAME' AS parameter_name, 'ALGO_RANDOM_FOREST' AS parameter_value FROM dual UNION ALL
         SELECT 'RF_NUM_TREES', '20' FROM dual UNION ALL
         SELECT 'RF_MAX_DEPTH', '8' FROM dual
       ) v
 WHERE p.algorithm = 'RANDOM_FOREST' AND p.version_tag = 'v1';

INSERT INTO iris_model_parameters (parameter_set_id, parameter_name, parameter_value)
SELECT p.parameter_set_id, v.parameter_name, v.parameter_value
  FROM iris_model_parameter_sets p
 CROSS JOIN
       ( SELECT 'ALGO_NAME' AS parameter_name, 'ALGO_RANDOM_FOREST' AS parameter_value FROM dual UNION ALL
         SELECT 'RF_NUM_TREES', '40' FROM dual UNION ALL
         SELECT 'RF_MAX_DEPTH', '12' FROM dual
       ) v
 WHERE p.algorithm = 'RANDOM_FOREST' AND p.version_tag = 'v2';

-- Support Vector Machine parameters (Oracle-specific setting names)
INSERT INTO iris_model_parameters (parameter_set_id, parameter_name, parameter_value)
SELECT p.parameter_set_id, v.parameter_name, v.parameter_value
  FROM iris_model_parameter_sets p
 CROSS JOIN
       ( SELECT 'ALGO_NAME' AS parameter_name, 'ALGO_SUPPORT_VECTOR_MACHINES' AS parameter_value FROM dual UNION ALL
         SELECT 'SVMS_COMPLEXITY_FACTOR', '0.5' FROM dual UNION ALL
         SELECT 'SVMS_KERNEL_FUNCTION', 'SVMS_LINEAR' FROM dual
       ) v
 WHERE p.algorithm = 'SVM' AND p.version_tag = 'v1';

INSERT INTO iris_model_parameters (parameter_set_id, parameter_name, parameter_value)
SELECT p.parameter_set_id, v.parameter_name, v.parameter_value
  FROM iris_model_parameter_sets p
 CROSS JOIN
       ( SELECT 'ALGO_NAME' AS parameter_name, 'ALGO_SUPPORT_VECTOR_MACHINES' AS parameter_value FROM dual UNION ALL
         SELECT 'SVMS_COMPLEXITY_FACTOR', '1.0' FROM dual UNION ALL
         SELECT 'SVMS_KERNEL_FUNCTION', 'SVMS_GAUSSIAN' FROM dual
       ) v
 WHERE p.algorithm = 'SVM' AND p.version_tag = 'v2';

-- Lifecycle and metadata tables
CREATE TABLE iris_model_lifecycle (
    model_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_name       VARCHAR2(128) NOT NULL,
    algorithm        VARCHAR2(50)  NOT NULL,
    fold_id          NUMBER        NOT NULL,
    parameter_set_id NUMBER        NOT NULL,
    version_tag      VARCHAR2(30)  NOT NULL,
    status           VARCHAR2(20)  DEFAULT 'ACTIVE',
    created_on       TIMESTAMP     DEFAULT SYSTIMESTAMP,
    next_retrain_on  TIMESTAMP,
    CONSTRAINT iris_model_lifecycle_fk FOREIGN KEY (parameter_set_id)
        REFERENCES iris_model_parameter_sets(parameter_set_id)
);

CREATE TABLE iris_metrics (
    metric_id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_id         NUMBER        NOT NULL,
    parameter_set_id NUMBER        NOT NULL,
    fold_id          NUMBER        NOT NULL,
    algorithm        VARCHAR2(50)  NOT NULL,
    hyperparameters  VARCHAR2(4000) NOT NULL,
    accuracy         NUMBER,
    precision_macro  NUMBER,
    recall_macro     NUMBER,
    f1_macro         NUMBER,
    roc_auc          NUMBER,
    lift_value       NUMBER,
    evaluated_on     TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT iris_metrics_model_fk FOREIGN KEY (model_id)
        REFERENCES iris_model_lifecycle(model_id)
);

CREATE TABLE iris_retraining_schedule (
    schedule_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_id         NUMBER NOT NULL,
    parameter_set_id NUMBER NOT NULL,
    fold_id          NUMBER NOT NULL,
    frequency_days   NUMBER NOT NULL,
    next_run_on      TIMESTAMP NOT NULL,
    last_trained_on  TIMESTAMP NOT NULL,
    notes            VARCHAR2(400),
    CONSTRAINT iris_retraining_model_fk FOREIGN KEY (model_id)
        REFERENCES iris_model_lifecycle(model_id)
);

-- Oracle-specific global temporary table for dynamic mining settings
CREATE GLOBAL TEMPORARY TABLE iris_dm_settings (
    setting_name  VARCHAR2(128),
    setting_value VARCHAR2(4000)
) ON COMMIT DELETE ROWS;

COMMIT;

-- #####################################################################
-- Section 5: Model training, evaluation, and lifecycle management
--         (Oracle-specific use of DBMS_DATA_MINING package)
-- #####################################################################
-- NOTE: For multi-class classification, ROC AUC and Lift metrics are computed
-- in a One-vs-Rest (OvR) manner for a single class ('Iris-virginica'). This
-- provides a partial view of model performance. For complete evaluation, these
-- metrics should ideally be computed for each class separately and averaged.
-- In contrast, precision, recall, and F1 are macro-averaged across all classes.
DECLARE
    c_positive_target    CONSTANT VARCHAR2(20) := 'Iris-virginica';
    v_accuracy           NUMBER;
    v_precision          NUMBER;
    v_recall             NUMBER;
    v_f1                 NUMBER;
    v_roc_auc            NUMBER;
    v_lift               NUMBER;
    v_model_name         VARCHAR2(128);
    v_model_id           NUMBER;
    v_train_table        VARCHAR2(30);
    v_test_table         VARCHAR2(30);
    v_apply_table        VARCHAR2(30);
    v_lift_table         VARCHAR2(30);
    v_probability_column VARCHAR2(128);
    v_hyperparameters    VARCHAR2(4000);
    v_has_positive       NUMBER;
    v_has_negative       NUMBER;
BEGIN
    FOR fold_rec IN (SELECT DISTINCT fold_id FROM iris_folds ORDER BY fold_id) LOOP
        v_train_table := DBMS_ASSERT.SIMPLE_SQL_NAME('IRIS_TRAIN_F' || fold_rec.fold_id);
        v_test_table  := DBMS_ASSERT.SIMPLE_SQL_NAME('IRIS_TEST_F' || fold_rec.fold_id);

        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_train_table;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE <> -942 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_test_table;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE <> -942 THEN
                    RAISE;
                END IF;
        END;

        EXECUTE IMMEDIATE 'CREATE TABLE ' || v_train_table || ' AS
            SELECT * FROM iris_folds WHERE fold_id <> ' || fold_rec.fold_id;

        EXECUTE IMMEDIATE 'CREATE TABLE ' || v_test_table || ' AS
            SELECT * FROM iris_folds WHERE fold_id = ' || fold_rec.fold_id;

        FOR param_rec IN (
            SELECT ps.parameter_set_id,
                   ps.algorithm,
                   ps.version_tag
              FROM iris_model_parameter_sets ps
             ORDER BY ps.algorithm, ps.version_tag
        ) LOOP
            EXECUTE IMMEDIATE 'TRUNCATE TABLE iris_dm_settings';

            INSERT INTO iris_dm_settings (setting_name, setting_value)
            SELECT parameter_name, parameter_value
              FROM iris_model_parameters
             WHERE parameter_set_id = param_rec.parameter_set_id;

            v_model_name := DBMS_ASSERT.SIMPLE_SQL_NAME('IRIS_' || param_rec.algorithm || '_F' || fold_rec.fold_id || '_' || param_rec.version_tag);

            BEGIN
                DBMS_DATA_MINING.DROP_MODEL(v_model_name);
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE NOT IN (-40108, -4043) THEN
                        RAISE;
                    END IF;
            END;

            DBMS_DATA_MINING.CREATE_MODEL(
                model_name           => v_model_name,
                mining_function      => DBMS_DATA_MINING.CLASSIFICATION,
                data_table_name      => v_train_table,
                case_id_column_name  => 'ID',
                target_column_name   => 'SPECIES',
                settings_table_name  => 'IRIS_DM_SETTINGS'
            );

            INSERT INTO iris_model_lifecycle (
                model_name,
                algorithm,
                fold_id,
                parameter_set_id,
                version_tag,
                status,
                next_retrain_on
            ) VALUES (
                v_model_name,
                param_rec.algorithm,
                fold_rec.fold_id,
                param_rec.parameter_set_id,
                param_rec.version_tag,
                'ACTIVE',
                SYSTIMESTAMP + INTERVAL '30' DAY
            ) RETURNING model_id INTO v_model_id;

            v_apply_table := DBMS_ASSERT.SIMPLE_SQL_NAME('IRIS_APPLY_' || v_model_id);
            -- Oracle-specific: prediction probability columns follow the pattern
            -- PROBABILITY_<TARGET_VALUE> with hyphens converted to underscores.
            v_probability_column := 'PROBABILITY_' || REPLACE(UPPER(c_positive_target), '-', '_');

            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE ' || v_apply_table;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE <> -942 THEN
                        RAISE;
                    END IF;
            END;

            DBMS_DATA_MINING.APPLY(
                model_name            => v_model_name,
                data_table_name       => v_test_table,
                case_id_column_name   => 'ID',
                result_table_name     => v_apply_table
            );

            DBMS_DATA_MINING.COMPUTE_CONFUSION_MATRIX(
                accuracy                 => v_accuracy,
                apply_result_table_name  => v_apply_table,
                target_table_name        => v_test_table,
                target_value_column_name => 'SPECIES',
                case_id_column_name      => 'ID',
                score_column_name        => 'PREDICTION'
            );

            -- Guard: check if test set has both positive and negative examples for ROC/Lift
            EXECUTE IMMEDIATE
                'SELECT NVL(MAX(CASE WHEN species = ''' || c_positive_target || ''' THEN 1 ELSE 0 END), 0),
                        NVL(MAX(CASE WHEN species <> ''' || c_positive_target || ''' THEN 1 ELSE 0 END), 0)
                   FROM ' || v_test_table
                INTO v_has_positive, v_has_negative;

            -- Only compute ROC if test set has both positive and negative examples
            IF v_has_positive > 0 AND v_has_negative > 0 THEN
                DBMS_DATA_MINING.COMPUTE_ROC(
                    roc_area                => v_roc_auc,
                    apply_result_table_name => v_apply_table,
                    target_table_name       => v_test_table,
                    target_value_column_name=> 'SPECIES',
                    case_id_column_name     => 'ID',
                    positive_target_value   => c_positive_target,
                    score_column_name       => v_probability_column
                );
            ELSE
                v_roc_auc := NULL;  -- Set to NULL when ROC cannot be computed
            END IF;

            v_lift_table := 'IRIS_LIFT_' || v_model_id;
            
            -- Only compute Lift if test set has both positive and negative examples
            IF v_has_positive > 0 AND v_has_negative > 0 THEN
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE ' || v_lift_table;
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE <> -942 THEN
                            RAISE;
                        END IF;
                END;

                DBMS_DATA_MINING.COMPUTE_LIFT(
                    apply_result_table_name  => v_apply_table,
                    target_table_name        => v_test_table,
                    target_value_column_name => 'SPECIES',
                    case_id_column_name      => 'ID',
                    positive_target_value    => c_positive_target,
                    num_bins                 => 10,
                    score_column_name        => v_probability_column,
                    lift_table_name          => v_lift_table
                );
            ELSE
                v_lift := NULL;  -- Set to NULL when Lift cannot be computed
            END IF;

            SELECT LISTAGG(parameter_name || '=' || parameter_value, '; ')
                     WITHIN GROUP (ORDER BY parameter_name)
              INTO v_hyperparameters
              FROM iris_model_parameters
             WHERE parameter_set_id = param_rec.parameter_set_id;

            WITH cm AS (
                SELECT actual_target_value    AS actual,
                       predicted_target_value AS predicted,
                       target_count           AS cnt
                  FROM dm$vconfusion_matrix
            ),
            per_class AS (
                SELECT base.actual AS class_label,
                       SUM(CASE WHEN base.actual = base.predicted THEN base.cnt ELSE 0 END) AS tp,
                       SUM(base.cnt) AS actual_total
                  FROM cm base
                 GROUP BY base.actual
            ),
            per_predicted AS (
                SELECT pred.predicted AS class_label,
                       SUM(pred.cnt) AS predicted_total
                  FROM cm pred
                 GROUP BY pred.predicted
            ),
            aggregates AS (
                SELECT AVG(CASE WHEN pp.predicted_total > 0 THEN pc.tp / pp.predicted_total ELSE 0 END) AS precision_macro,
                       AVG(CASE WHEN pc.actual_total > 0 THEN pc.tp / pc.actual_total ELSE 0 END) AS recall_macro
                  FROM per_class pc
                  LEFT JOIN per_predicted pp ON pc.class_label = pp.class_label
            )
            SELECT precision_macro,
                   recall_macro,
                   CASE
                       WHEN precision_macro + recall_macro = 0 THEN 0
                       ELSE 2 * precision_macro * recall_macro /
                            (precision_macro + recall_macro)
                   END AS f1_macro
              INTO v_precision, v_recall, v_f1
              FROM aggregates;

            -- Only extract lift value if it was computed
            IF v_has_positive > 0 AND v_has_negative > 0 THEN
                EXECUTE IMMEDIATE
                    'SELECT lift_value FROM (
                         SELECT bucket_number, lift_value
                           FROM ' || v_lift_table || '
                          ORDER BY bucket_number
                     ) WHERE ROWNUM = 1'
                    INTO v_lift;
            END IF;

            INSERT INTO iris_metrics (
                model_id,
                parameter_set_id,
                fold_id,
                algorithm,
                hyperparameters,
                accuracy,
                precision_macro,
                recall_macro,
                f1_macro,
                roc_auc,
                lift_value
            ) VALUES (
                v_model_id,
                param_rec.parameter_set_id,
                fold_rec.fold_id,
                param_rec.algorithm,
                v_hyperparameters,
                v_accuracy,
                v_precision,
                v_recall,
                v_f1,
                v_roc_auc,
                v_lift
            );

            INSERT INTO iris_retraining_schedule (
                model_id,
                parameter_set_id,
                fold_id,
                frequency_days,
                next_run_on,
                last_trained_on,
                notes
            ) VALUES (
                v_model_id,
                param_rec.parameter_set_id,
                fold_rec.fold_id,
                30,
                SYSTIMESTAMP + INTERVAL '30' DAY,
                SYSTIMESTAMP,
                'Auto-generated from training job'
            );
        END LOOP;
    END LOOP;
    
    COMMIT;
END;
/

-- ###########################################################
-- Section 6: Persisted metadata for downstream consumption
-- ###########################################################
CREATE OR REPLACE VIEW iris_model_summary AS
SELECT l.model_id,
       l.model_name,
       l.algorithm,
       l.fold_id,
       l.version_tag,
       l.status,
       l.created_on,
       l.next_retrain_on,
       m.accuracy,
       m.precision_macro,
       m.recall_macro,
       m.f1_macro,
       m.roc_auc,
       m.lift_value
  FROM iris_model_lifecycle l
  JOIN iris_metrics m
    ON m.model_id = l.model_id;

CREATE OR REPLACE VIEW iris_retraining_overview AS
SELECT r.model_id,
       l.model_name,
       r.frequency_days,
       r.next_run_on,
       r.last_trained_on,
       r.notes
  FROM iris_retraining_schedule r
  JOIN iris_model_lifecycle l
    ON l.model_id = r.model_id;

COMMIT;
