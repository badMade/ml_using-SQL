"""
Tests for schema consistency across all database platforms.

Ensures that all platforms implement the same logical schema for the ml_metrics table,
with the same columns and primary key structure.
"""
import pytest
from pathlib import Path
from typing import List, Tuple

from conftest import (
    PLATFORMS,
    REQUIRED_COLUMNS,
    PRIMARY_KEY_COLUMNS,
    get_ddl_files,
    read_sql_file,
    extract_columns_from_ddl,
    extract_primary_key_from_ddl,
)


class TestDDLFilesExist:
    """Test that all required DDL files exist for each platform."""

    @pytest.mark.parametrize("platform", PLATFORMS)
    def test_ddl_file_exists(self, project_root: Path, platform: str):
        """Each platform should have a create_metrics_table.sql file."""
        ddl_path = project_root / 'sql' / platform / 'create_metrics_table.sql'
        assert ddl_path.exists(), (
            f"Missing DDL file for {platform}: {ddl_path}"
        )

    @pytest.mark.parametrize("platform", PLATFORMS)
    def test_ddl_file_not_empty(self, project_root: Path, platform: str):
        """Each DDL file should have content."""
        ddl_path = project_root / 'sql' / platform / 'create_metrics_table.sql'
        if ddl_path.exists():
            content = read_sql_file(ddl_path)
            assert len(content.strip()) > 0, (
                f"DDL file for {platform} is empty"
            )


class TestSchemaColumns:
    """Test that all platforms have the required schema columns."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_has_all_required_columns(self, platform: str, ddl_path: Path):
        """Each platform's DDL should define all required columns."""
        content = read_sql_file(ddl_path)
        columns = extract_columns_from_ddl(content)

        missing = [col for col in REQUIRED_COLUMNS if col not in columns]
        assert len(missing) == 0, (
            f"Platform {platform} is missing columns: {missing}"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_column_count_matches(self, platform: str, ddl_path: Path):
        """Each platform should have exactly 10 columns."""
        content = read_sql_file(ddl_path)
        columns = extract_columns_from_ddl(content)

        assert len(columns) == 10, (
            f"Platform {platform} has {len(columns)} columns, expected 10. "
            f"Found: {columns}"
        )


class TestPrimaryKey:
    """Test that all platforms have the correct primary key structure."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_has_primary_key(self, platform: str, ddl_path: Path):
        """Each platform's DDL should define a primary key."""
        content = read_sql_file(ddl_path)
        pk_cols = extract_primary_key_from_ddl(content)

        assert len(pk_cols) > 0, (
            f"Platform {platform} DDL does not define a PRIMARY KEY"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_primary_key_columns(self, platform: str, ddl_path: Path):
        """Primary key should be on (pipeline_id, run_id, metric_name)."""
        content = read_sql_file(ddl_path)
        pk_cols = extract_primary_key_from_ddl(content)

        assert set(pk_cols) == set(PRIMARY_KEY_COLUMNS), (
            f"Platform {platform} has incorrect primary key columns. "
            f"Expected {PRIMARY_KEY_COLUMNS}, got {pk_cols}"
        )


class TestTableName:
    """Test that all platforms use the correct table name."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_table_name_is_ml_metrics(self, platform: str, ddl_path: Path):
        """Table name should be ml_metrics (allowing for schema prefixes)."""
        content = read_sql_file(ddl_path)

        # Match CREATE TABLE with optional schema prefix
        assert 'ml_metrics' in content.lower(), (
            f"Platform {platform} DDL does not create 'ml_metrics' table"
        )


class TestNotNullConstraints:
    """Test that required columns have NOT NULL constraints."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_pipeline_id_not_null(self, platform: str, ddl_path: Path):
        """pipeline_id should have NOT NULL constraint."""
        content = read_sql_file(ddl_path)
        # Check that pipeline_id line contains NOT NULL
        assert _column_has_not_null(content, 'pipeline_id'), (
            f"Platform {platform}: pipeline_id should be NOT NULL"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_run_id_not_null(self, platform: str, ddl_path: Path):
        """run_id should have NOT NULL constraint."""
        content = read_sql_file(ddl_path)
        assert _column_has_not_null(content, 'run_id'), (
            f"Platform {platform}: run_id should be NOT NULL"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_metric_name_not_null(self, platform: str, ddl_path: Path):
        """metric_name should have NOT NULL constraint."""
        content = read_sql_file(ddl_path)
        assert _column_has_not_null(content, 'metric_name'), (
            f"Platform {platform}: metric_name should be NOT NULL"
        )


class TestDefaultValues:
    """Test that columns with default values are properly defined."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_metric_type_has_default(self, platform: str, ddl_path: Path):
        """metric_type should have a default value of 'scalar'."""
        content = read_sql_file(ddl_path)
        assert _column_has_default(content, 'metric_type', 'scalar'), (
            f"Platform {platform}: metric_type should have DEFAULT 'scalar'"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_recorded_at_has_default(self, platform: str, ddl_path: Path):
        """recorded_at should have a timestamp default."""
        content = read_sql_file(ddl_path)
        # Various platforms use different timestamp functions
        timestamp_defaults = [
            'current_timestamp',
            'systimestamp',
            'sysdatetimeoffset',
            'getdate',
        ]
        has_default = any(
            _column_has_default(content, 'recorded_at', default)
            for default in timestamp_defaults
        )
        assert has_default, (
            f"Platform {platform}: recorded_at should have a timestamp default"
        )


class TestCrossplatformConsistency:
    """Test consistency across all platforms."""

    def test_all_platforms_have_same_columns(self, ddl_files):
        """All platforms should define the same set of columns."""
        platform_columns = {}

        for platform, ddl_path in ddl_files:
            content = read_sql_file(ddl_path)
            columns = extract_columns_from_ddl(content)
            platform_columns[platform] = set(columns)

        # Get first platform's columns as reference
        if not platform_columns:
            pytest.skip("No DDL files found")

        reference_platform = list(platform_columns.keys())[0]
        reference_columns = platform_columns[reference_platform]

        for platform, columns in platform_columns.items():
            assert columns == reference_columns, (
                f"Platform {platform} has different columns than {reference_platform}. "
                f"Difference: {columns.symmetric_difference(reference_columns)}"
            )

    def test_all_platforms_have_same_primary_key(self, ddl_files):
        """All platforms should have the same primary key structure."""
        platform_pk = {}

        for platform, ddl_path in ddl_files:
            content = read_sql_file(ddl_path)
            pk_cols = extract_primary_key_from_ddl(content)
            platform_pk[platform] = set(pk_cols)

        if not platform_pk:
            pytest.skip("No DDL files found")

        reference_platform = list(platform_pk.keys())[0]
        reference_pk = platform_pk[reference_platform]

        for platform, pk in platform_pk.items():
            assert pk == reference_pk, (
                f"Platform {platform} has different primary key than {reference_platform}. "
                f"Expected {reference_pk}, got {pk}"
            )


# Helper functions

def _column_has_not_null(sql_content: str, column_name: str) -> bool:
    """Check if a column definition includes NOT NULL."""
    for line in sql_content.split('\n'):
        if column_name.lower() in line.lower():
            return 'not null' in line.lower()
    return False


def _column_has_default(sql_content: str, column_name: str, default_value: str) -> bool:
    """Check if a column definition includes a specific default value."""
    for line in sql_content.split('\n'):
        if column_name.lower() in line.lower():
            return default_value.lower() in line.lower()
    return False
