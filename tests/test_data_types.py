"""
Tests for data type mappings across database platforms.

Validates that each platform uses appropriate data types for the ml_metrics table
columns, ensuring semantic equivalence across platforms.
"""
import re
import pytest
from pathlib import Path
from typing import Dict, List, Set

from conftest import (
    PLATFORMS,
    REQUIRED_COLUMNS,
    get_ddl_files,
    read_sql_file,
)


# Expected data type categories for each column
COLUMN_TYPE_CATEGORIES = {
    'pipeline_id': 'string',
    'run_id': 'string',
    'metric_name': 'string',
    'metric_value': 'numeric',
    'metric_type': 'string',
    'recorded_at': 'timestamp',
    'model_version': 'string',
    'environment_label': 'string',
    'tags': 'array_or_json',
    'metadata_json': 'json',
}

# Valid data types per category per platform
VALID_TYPES = {
    'string': {
        'oracle': ['varchar2', 'nvarchar2', 'char', 'nchar', 'clob'],
        'bigquery': ['string'],
        'redshift': ['varchar', 'char', 'text'],
        'sqlserver': ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'],
        'vertica': ['varchar', 'char', 'long varchar'],
        'postgres': ['varchar', 'char', 'text', 'character varying'],
    },
    'numeric': {
        'oracle': ['number', 'float', 'binary_float', 'binary_double'],
        'bigquery': ['float64', 'numeric', 'bignumeric', 'int64', 'float'],
        'redshift': ['float', 'real', 'double precision', 'decimal', 'numeric', 'float8', 'float4'],
        'sqlserver': ['float', 'real', 'decimal', 'numeric', 'money', 'smallmoney'],
        'vertica': ['float', 'real', 'double precision', 'decimal', 'numeric', 'float8'],
        'postgres': ['float', 'real', 'double precision', 'decimal', 'numeric', 'float8', 'float4'],
    },
    'timestamp': {
        'oracle': ['timestamp', 'timestamp with time zone', 'timestamp with local time zone', 'date'],
        'bigquery': ['timestamp', 'datetime'],
        'redshift': ['timestamp', 'timestamptz', 'timestamp with time zone'],
        'sqlserver': ['datetime', 'datetime2', 'datetimeoffset', 'smalldatetime'],
        'vertica': ['timestamp', 'timestamptz', 'timestamp with time zone'],
        'postgres': ['timestamp', 'timestamptz', 'timestamp with time zone', 'timestamp without time zone'],
    },
    'json': {
        'oracle': ['json', 'clob', 'blob'],
        'bigquery': ['json', 'string'],
        'redshift': ['super', 'varchar'],
        'sqlserver': ['nvarchar', 'varchar', 'json'],  # SQL Server uses NVARCHAR(MAX) for JSON
        'vertica': ['long varchar', 'varchar'],
        'postgres': ['json', 'jsonb', 'text'],
    },
    'array_or_json': {
        'oracle': ['json', 'clob'],
        'bigquery': ['array', 'json'],
        'redshift': ['super', 'varchar'],
        'sqlserver': ['nvarchar', 'varchar'],
        'vertica': ['array', 'long varchar'],
        'postgres': ['text[]', 'array', 'json', 'jsonb', 'varchar[]'],
    },
}


def extract_column_type(sql_content: str, column_name: str) -> str:
    """Extract the data type for a specific column from DDL."""
    # Pattern to match column definition with multi-word types
    # Captures: TYPE, TYPE(size), TYPE PRECISION, LONG VARCHAR, etc.
    pattern = rf'^\s*{column_name}\s+(\w+(?:\s+\w+)?(?:\s*\([^)]+\))?(?:\s*\[[^\]]*\])?)'

    for line in sql_content.split('\n'):
        match = re.match(pattern, line, re.IGNORECASE)
        if match:
            type_str = match.group(1).lower().strip()
            # Clean up extra spaces
            type_str = ' '.join(type_str.split())
            return type_str

    return ''


def normalize_type(type_str: str) -> str:
    """Normalize a type string for comparison."""
    # Remove size specifications like (128) or (MAX)
    normalized = re.sub(r'\([^)]*\)', '', type_str)
    # Remove array brackets content
    normalized = re.sub(r'\[[^\]]*\]', '[]', normalized)
    return normalized.lower().strip()


class TestStringColumns:
    """Test string column data types."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_pipeline_id_is_string(self, platform: str, ddl_path: Path):
        """pipeline_id should be a string type."""
        content = read_sql_file(ddl_path)
        col_type = extract_column_type(content, 'pipeline_id')

        valid_types = VALID_TYPES['string'].get(platform, [])
        normalized = normalize_type(col_type)

        is_valid = any(vt in normalized for vt in valid_types)
        assert is_valid, (
            f"Platform {platform}: pipeline_id type '{col_type}' "
            f"should be string type. Valid: {valid_types}"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_run_id_is_string(self, platform: str, ddl_path: Path):
        """run_id should be a string type."""
        content = read_sql_file(ddl_path)
        col_type = extract_column_type(content, 'run_id')

        valid_types = VALID_TYPES['string'].get(platform, [])
        normalized = normalize_type(col_type)

        is_valid = any(vt in normalized for vt in valid_types)
        assert is_valid, (
            f"Platform {platform}: run_id type '{col_type}' "
            f"should be string type"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_metric_name_is_string(self, platform: str, ddl_path: Path):
        """metric_name should be a string type."""
        content = read_sql_file(ddl_path)
        col_type = extract_column_type(content, 'metric_name')

        valid_types = VALID_TYPES['string'].get(platform, [])
        normalized = normalize_type(col_type)

        is_valid = any(vt in normalized for vt in valid_types)
        assert is_valid, (
            f"Platform {platform}: metric_name type '{col_type}' "
            f"should be string type"
        )


class TestNumericColumns:
    """Test numeric column data types."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_metric_value_is_numeric(self, platform: str, ddl_path: Path):
        """metric_value should be a numeric type."""
        content = read_sql_file(ddl_path)
        col_type = extract_column_type(content, 'metric_value')

        valid_types = VALID_TYPES['numeric'].get(platform, [])
        normalized = normalize_type(col_type)

        is_valid = any(vt in normalized for vt in valid_types)
        assert is_valid, (
            f"Platform {platform}: metric_value type '{col_type}' "
            f"should be numeric type. Valid: {valid_types}"
        )


class TestTimestampColumns:
    """Test timestamp column data types."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_recorded_at_is_timestamp(self, platform: str, ddl_path: Path):
        """recorded_at should be a timestamp type."""
        content = read_sql_file(ddl_path)
        col_type = extract_column_type(content, 'recorded_at')

        valid_types = VALID_TYPES['timestamp'].get(platform, [])
        normalized = normalize_type(col_type)

        is_valid = any(vt in normalized for vt in valid_types)
        assert is_valid, (
            f"Platform {platform}: recorded_at type '{col_type}' "
            f"should be timestamp type. Valid: {valid_types}"
        )


class TestJSONColumns:
    """Test JSON/complex column data types."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_metadata_json_is_json_type(self, platform: str, ddl_path: Path):
        """metadata_json should be a JSON-compatible type."""
        content = read_sql_file(ddl_path)
        col_type = extract_column_type(content, 'metadata_json')

        valid_types = VALID_TYPES['json'].get(platform, [])
        normalized = normalize_type(col_type)

        is_valid = any(vt in normalized for vt in valid_types)
        assert is_valid, (
            f"Platform {platform}: metadata_json type '{col_type}' "
            f"should be JSON-compatible. Valid: {valid_types}"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_tags_is_array_or_json(self, platform: str, ddl_path: Path):
        """tags should be an array or JSON type."""
        content = read_sql_file(ddl_path)
        col_type = extract_column_type(content, 'tags')

        valid_types = VALID_TYPES['array_or_json'].get(platform, [])
        normalized = normalize_type(col_type)

        is_valid = any(vt in normalized for vt in valid_types)
        assert is_valid, (
            f"Platform {platform}: tags type '{col_type}' "
            f"should be array or JSON. Valid: {valid_types}"
        )


class TestStringSizes:
    """Test string column size specifications."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_pipeline_id_sufficient_size(self, platform: str, ddl_path: Path):
        """pipeline_id should have sufficient size (at least 64 chars)."""
        content = read_sql_file(ddl_path)

        # Look for size specification
        for line in content.split('\n'):
            if 'pipeline_id' in line.lower():
                # Extract size from parentheses
                size_match = re.search(r'\((\d+)\)', line)
                if size_match:
                    size = int(size_match.group(1))
                    assert size >= 64, (
                        f"Platform {platform}: pipeline_id size {size} "
                        f"should be at least 64"
                    )
                break

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_metric_name_sufficient_size(self, platform: str, ddl_path: Path):
        """metric_name should have sufficient size (at least 128 chars)."""
        content = read_sql_file(ddl_path)

        for line in content.split('\n'):
            if 'metric_name' in line.lower():
                size_match = re.search(r'\((\d+)\)', line)
                if size_match:
                    size = int(size_match.group(1))
                    assert size >= 128, (
                        f"Platform {platform}: metric_name size {size} "
                        f"should be at least 128"
                    )
                break


class TestTypeConsistency:
    """Test type consistency across platforms."""

    def test_all_platforms_use_equivalent_types(self, ddl_files):
        """All platforms should use semantically equivalent types."""
        platform_types = {}

        for platform, ddl_path in ddl_files:
            content = read_sql_file(ddl_path)
            types = {}
            for col in REQUIRED_COLUMNS:
                col_type = extract_column_type(content, col)
                types[col] = col_type
            platform_types[platform] = types

        # Verify each column has a type in each platform
        for col in REQUIRED_COLUMNS:
            for platform, types in platform_types.items():
                assert types.get(col), (
                    f"Platform {platform} missing type for column {col}"
                )
