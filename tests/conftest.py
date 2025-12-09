"""
Pytest configuration and fixtures for ML Metrics SQL Toolkit tests.
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pytest


# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Supported database platforms
PLATFORMS = ['oracle', 'bigquery', 'redshift', 'sqlserver', 'vertica', 'postgres']

# Required schema columns for ml_metrics table
REQUIRED_COLUMNS = [
    'pipeline_id',
    'run_id',
    'metric_name',
    'metric_value',
    'metric_type',
    'recorded_at',
    'model_version',
    'environment_label',
    'tags',
    'metadata_json',
]

# Primary key columns
PRIMARY_KEY_COLUMNS = ['pipeline_id', 'run_id', 'metric_name']

# Dialect mapping for sqlfluff
DIALECT_MAP = {
    'oracle': 'oracle',
    'bigquery': 'bigquery',
    'redshift': 'redshift',
    'sqlserver': 'tsql',
    'vertica': 'postgres',
    'postgres': 'postgres',
}


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return PROJECT_ROOT


@pytest.fixture
def platforms() -> List[str]:
    """Return list of supported database platforms."""
    return PLATFORMS


@pytest.fixture
def required_columns() -> List[str]:
    """Return list of required schema columns."""
    return REQUIRED_COLUMNS


@pytest.fixture
def primary_key_columns() -> List[str]:
    """Return list of primary key columns."""
    return PRIMARY_KEY_COLUMNS


@pytest.fixture
def dialect_map() -> Dict[str, str]:
    """Return dialect mapping for sqlfluff."""
    return DIALECT_MAP


def get_ddl_files() -> List[Tuple[str, Path]]:
    """Get all DDL files with their platform names."""
    files = []
    for platform in PLATFORMS:
        ddl_path = PROJECT_ROOT / 'sql' / platform / 'create_metrics_table.sql'
        if ddl_path.exists():
            files.append((platform, ddl_path))
    return files


def get_workflow_files() -> List[Tuple[str, Path]]:
    """Get all ML workflow files with their platform names."""
    files = []
    for platform in PLATFORMS:
        workflow_path = PROJECT_ROOT / platform / 'iris_ml_workflow.sql'
        if workflow_path.exists():
            files.append((platform, workflow_path))
    return files


def get_all_sql_files() -> List[Tuple[str, Path]]:
    """Get all SQL files with their platform names."""
    return get_ddl_files() + get_workflow_files()


@pytest.fixture
def ddl_files() -> List[Tuple[str, Path]]:
    """Return list of DDL files as (platform, path) tuples."""
    return get_ddl_files()


@pytest.fixture
def workflow_files() -> List[Tuple[str, Path]]:
    """Return list of workflow files as (platform, path) tuples."""
    return get_workflow_files()


@pytest.fixture
def all_sql_files() -> List[Tuple[str, Path]]:
    """Return all SQL files as (platform, path) tuples."""
    return get_all_sql_files()


def read_sql_file(path: Path) -> str:
    """Read and return contents of a SQL file."""
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def extract_columns_from_ddl(sql_content: str) -> List[str]:
    """
    Extract column names from a CREATE TABLE DDL statement.
    Returns list of column names in lowercase.
    """
    columns = []
    # Match column definitions (name followed by type)
    # Pattern handles various SQL dialects
    pattern = r'^\s+(\w+)\s+(?:VARCHAR|NVARCHAR|VARCHAR2|STRING|TEXT|INT|INTEGER|FLOAT|DOUBLE|NUMBER|TIMESTAMP|ARRAY|JSON|JSONB|SUPER|LONG|DATETIME)'

    for line in sql_content.split('\n'):
        match = re.match(pattern, line, re.IGNORECASE)
        if match:
            columns.append(match.group(1).lower())

    return columns


def extract_primary_key_from_ddl(sql_content: str) -> List[str]:
    """
    Extract primary key columns from a CREATE TABLE DDL statement.
    Returns list of column names in lowercase.
    """
    # Match PRIMARY KEY constraint - handles various syntaxes:
    # - PRIMARY KEY (col1, col2)
    # - PRIMARY KEY CLUSTERED (col1, col2)
    # - CONSTRAINT name PRIMARY KEY (col1, col2)
    # - CONSTRAINT name PRIMARY KEY CLUSTERED (col1, col2)
    pk_pattern = r'PRIMARY\s+KEY(?:\s+CLUSTERED)?\s*\(([^)]+)\)'
    match = re.search(pk_pattern, sql_content, re.IGNORECASE)

    if match:
        pk_cols = match.group(1)
        # Split by comma and clean up
        return [col.strip().lower() for col in pk_cols.split(',')]

    return []


@pytest.fixture
def read_sql():
    """Fixture that returns the read_sql_file function."""
    return read_sql_file


@pytest.fixture
def extract_columns():
    """Fixture that returns the extract_columns_from_ddl function."""
    return extract_columns_from_ddl


@pytest.fixture
def extract_primary_key():
    """Fixture that returns the extract_primary_key_from_ddl function."""
    return extract_primary_key_from_ddl
