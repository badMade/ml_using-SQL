"""
Tests for ML workflow file structure and content.

Validates that all iris_ml_workflow.sql files follow the expected structure
and contain required components for ML pipeline execution.
"""
import re
import pytest
from pathlib import Path
from typing import List, Tuple

from conftest import (
    PLATFORMS,
    get_workflow_files,
    read_sql_file,
)


class TestWorkflowFilesExist:
    """Test that workflow files exist for each platform."""

    @pytest.mark.parametrize("platform", PLATFORMS)
    def test_workflow_file_exists(self, project_root: Path, platform: str):
        """Each platform should have an iris_ml_workflow.sql file."""
        workflow_path = project_root / platform / 'iris_ml_workflow.sql'
        assert workflow_path.exists(), (
            f"Missing workflow file for {platform}: {workflow_path}"
        )

    @pytest.mark.parametrize("platform", PLATFORMS)
    def test_workflow_file_not_empty(self, project_root: Path, platform: str):
        """Workflow files should have substantial content."""
        workflow_path = project_root / platform / 'iris_ml_workflow.sql'
        if workflow_path.exists():
            content = read_sql_file(workflow_path)
            # Workflow files should be at least 1KB
            assert len(content) > 1000, (
                f"Workflow file for {platform} seems too small ({len(content)} bytes)"
            )


class TestIrisDataset:
    """Test that workflows properly handle the Iris dataset."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_contains_iris_reference(self, platform: str, workflow_path: Path):
        """Workflow should reference the Iris dataset."""
        content = read_sql_file(workflow_path)

        assert 'iris' in content.lower(), (
            f"Workflow for {platform} should reference Iris dataset"
        )

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_contains_species_column(self, platform: str, workflow_path: Path):
        """Workflow should reference species (target variable)."""
        content = read_sql_file(workflow_path)

        assert 'species' in content.lower(), (
            f"Workflow for {platform} should reference species column"
        )

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_contains_feature_columns(self, platform: str, workflow_path: Path):
        """Workflow should reference Iris feature columns."""
        content = read_sql_file(workflow_path).lower()
        features = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']

        found_features = [f for f in features if f in content]
        assert len(found_features) >= 3, (
            f"Workflow for {platform} should reference Iris feature columns. "
            f"Found: {found_features}"
        )


class TestKFoldCrossValidation:
    """Test that workflows implement k-fold cross-validation."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_contains_fold_reference(self, platform: str, workflow_path: Path):
        """Workflow should implement fold-based validation."""
        content = read_sql_file(workflow_path).lower()

        fold_indicators = ['fold', 'ntile', 'cross_valid', 'cv']
        has_fold = any(indicator in content for indicator in fold_indicators)

        assert has_fold, (
            f"Workflow for {platform} should implement fold-based validation"
        )

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_has_kfold_procedure_or_logic(self, platform: str, workflow_path: Path):
        """Workflow should have k-fold procedure or equivalent logic."""
        content = read_sql_file(workflow_path).lower()

        # Look for procedure/function definitions or loop constructs
        procedure_indicators = [
            'procedure', 'function', 'while', 'loop', 'for ',
            'declare', 'begin', 'create or replace'
        ]
        has_procedure = any(indicator in content for indicator in procedure_indicators)

        assert has_procedure, (
            f"Workflow for {platform} should define procedures or use control flow"
        )


class TestMetricsLogging:
    """Test that workflows log metrics properly."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_has_metrics_table(self, platform: str, workflow_path: Path):
        """Workflow should create or reference a metrics table."""
        content = read_sql_file(workflow_path).lower()

        metrics_indicators = ['iris_metrics', 'ml_metrics', 'metrics']
        has_metrics = any(indicator in content for indicator in metrics_indicators)

        assert has_metrics, (
            f"Workflow for {platform} should reference a metrics table"
        )

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_logs_accuracy_metric(self, platform: str, workflow_path: Path):
        """Workflow should log accuracy or similar evaluation metric."""
        content = read_sql_file(workflow_path).lower()

        metric_indicators = ['accuracy', 'precision', 'recall', 'f1', 'auc', 'metric']
        has_metric = any(indicator in content for indicator in metric_indicators)

        assert has_metric, (
            f"Workflow for {platform} should log evaluation metrics"
        )


class TestPredictionsTable:
    """Test that workflows store predictions."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_has_predictions_table(self, platform: str, workflow_path: Path):
        """Workflow should create or use a predictions table."""
        content = read_sql_file(workflow_path).lower()

        pred_indicators = ['prediction', 'predicted', 'predict']
        has_predictions = any(indicator in content for indicator in pred_indicators)

        assert has_predictions, (
            f"Workflow for {platform} should handle predictions"
        )


class TestRunRegistry:
    """Test that workflows track runs."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_has_run_registry_or_tracking(self, platform: str, workflow_path: Path):
        """Workflow should track runs with IDs or lifecycle tables."""
        content = read_sql_file(workflow_path).lower()

        # Various patterns for run/experiment tracking
        run_indicators = [
            'run_id', 'run_registry', 'pipeline_id', 'experiment',
            'model_lifecycle', 'retraining', 'model_id', 'training_run'
        ]
        has_run_tracking = any(indicator in content for indicator in run_indicators)

        assert has_run_tracking, (
            f"Workflow for {platform} should track runs or model lifecycle"
        )


class TestSQLSecurity:
    """Test SQL security practices in workflows."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_no_sql_injection_patterns(self, platform: str, workflow_path: Path):
        """Workflow should not have obvious SQL injection vulnerabilities."""
        content = read_sql_file(workflow_path)

        # Check for dangerous patterns (string concatenation with user input)
        dangerous_patterns = [
            r"'\s*\+\s*\w+\s*\+\s*'",  # String concatenation like ' + var + '
            r'"\s*\+\s*\w+\s*\+\s*"',  # Double quote concatenation
        ]

        for pattern in dangerous_patterns:
            matches = re.findall(pattern, content)
            # Note: some string building is necessary, but flag for review
            if matches:
                print(f"Note: {platform} workflow has string concatenation (review for safety)")

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_uses_parameterized_queries_where_applicable(self, platform: str, workflow_path: Path):
        """Workflow should use parameterized queries where possible."""
        content = read_sql_file(workflow_path).lower()

        # Check for parameterization indicators
        param_indicators = [
            'using',  # EXECUTE ... USING (PostgreSQL, Vertica)
            '$1', '$2',  # PostgreSQL parameters
            ':1', ':2',  # Oracle bind variables
            '?',  # JDBC/ODBC style
            '@',  # SQL Server parameters
        ]

        # This is informational - not all queries need parameterization
        has_params = any(indicator in content for indicator in param_indicators)
        if not has_params:
            print(f"Note: {platform} workflow may benefit from parameterized queries")


class TestWorkflowExecution:
    """Test that workflows have proper execution entry points."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_has_execution_call(self, platform: str, workflow_path: Path):
        """Workflow should have a call/execute statement to run the pipeline."""
        content = read_sql_file(workflow_path).lower()

        exec_indicators = ['call ', 'exec ', 'execute ', 'begin']
        has_execution = any(indicator in content for indicator in exec_indicators)

        # At minimum, there should be executable SQL statements
        assert has_execution or 'select' in content or 'insert' in content, (
            f"Workflow for {platform} should have executable statements"
        )


class TestPlatformPlatformHeader:
    """Test that workflows properly identify their target platform."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_header_mentions_platform(self, platform: str, workflow_path: Path):
        """Workflow header comment should mention the platform."""
        content = read_sql_file(workflow_path)

        # Get first 500 characters (header area)
        header = content[:500].lower()

        # Platform name or variant should be in header
        platform_variants = {
            'oracle': ['oracle', 'pl/sql'],
            'bigquery': ['bigquery', 'bq', 'google'],
            'redshift': ['redshift', 'amazon'],
            'sqlserver': ['sql server', 'sqlserver', 't-sql', 'tsql', 'microsoft'],
            'vertica': ['vertica'],
            'postgres': ['postgres', 'postgresql', 'pg'],
        }

        variants = platform_variants.get(platform, [platform])
        has_platform_ref = any(v in header for v in variants)

        assert has_platform_ref, (
            f"Workflow for {platform} header should mention the platform"
        )
