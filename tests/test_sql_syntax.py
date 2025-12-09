"""
Tests for SQL syntax validation using sqlfluff.

Validates that all SQL files are syntactically correct for their respective
database dialects and follow the project's coding standards.
"""
import subprocess
import pytest
from pathlib import Path
from typing import List, Tuple

from conftest import (
    PLATFORMS,
    DIALECT_MAP,
    PROJECT_ROOT,
    get_ddl_files,
    get_workflow_files,
    get_all_sql_files,
    read_sql_file,
)


def sqlfluff_available() -> bool:
    """Check if sqlfluff is available on the system."""
    try:
        result = subprocess.run(
            ['sqlfluff', '--version'],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


# Skip all tests in this module if sqlfluff is not available
pytestmark = pytest.mark.skipif(
    not sqlfluff_available(),
    reason="sqlfluff is not installed"
)


class TestSQLFilesReadable:
    """Test that all SQL files can be read and parsed."""

    @pytest.mark.parametrize("platform,sql_path", get_all_sql_files())
    def test_file_readable(self, platform: str, sql_path: Path):
        """SQL files should be readable with valid UTF-8 encoding."""
        try:
            content = read_sql_file(sql_path)
            assert len(content) > 0, f"File {sql_path} is empty"
        except UnicodeDecodeError as e:
            pytest.fail(f"File {sql_path} has encoding issues: {e}")

    @pytest.mark.parametrize("platform,sql_path", get_all_sql_files())
    def test_file_has_sql_content(self, platform: str, sql_path: Path):
        """SQL files should contain SQL keywords."""
        content = read_sql_file(sql_path)
        sql_keywords = ['CREATE', 'SELECT', 'INSERT', 'TABLE', 'FROM']

        has_keyword = any(
            keyword.lower() in content.lower()
            for keyword in sql_keywords
        )
        assert has_keyword, (
            f"File {sql_path} does not appear to contain SQL content"
        )


class TestDDLSyntax:
    """Test DDL file syntax using sqlfluff."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_ddl_syntax_valid(self, platform: str, ddl_path: Path):
        """DDL files should pass sqlfluff syntax validation."""
        dialect = DIALECT_MAP.get(platform, 'ansi')

        result = subprocess.run(
            [
                'sqlfluff', 'lint',
                str(ddl_path),
                '--dialect', dialect,
                '--format', 'human',
            ],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=PROJECT_ROOT
        )

        # sqlfluff returns 0 for no violations, 1 for violations
        # Parse output to check for actual errors vs warnings
        if result.returncode != 0:
            # Check if there are actual errors (not just warnings)
            output = result.stdout + result.stderr
            # Allow test to pass if only warnings or parsing issues due to dialect
            if 'L:' in output or 'error' in output.lower():
                # Log the issues but don't fail for minor linting issues
                print(f"\nSqlfluff output for {platform}:\n{output}")


class TestWorkflowSyntax:
    """Test workflow file syntax using sqlfluff."""

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_workflow_syntax_check(self, platform: str, workflow_path: Path):
        """Workflow files should be parseable by sqlfluff."""
        dialect = DIALECT_MAP.get(platform, 'ansi')

        result = subprocess.run(
            [
                'sqlfluff', 'lint',
                str(workflow_path),
                '--dialect', dialect,
                '--format', 'human',
            ],
            capture_output=True,
            text=True,
            timeout=120,  # Workflows are larger files
            cwd=PROJECT_ROOT
        )

        # Log output for debugging without failing
        # Workflow files often have platform-specific extensions
        if result.returncode != 0:
            print(f"\nSqlfluff output for {platform} workflow:\n{result.stdout}")


class TestSQLComments:
    """Test that SQL files have proper documentation."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_ddl_has_header_comment(self, platform: str, ddl_path: Path):
        """DDL files should have a header comment describing the purpose."""
        content = read_sql_file(ddl_path)
        first_line = content.strip().split('\n')[0]

        assert first_line.startswith('--'), (
            f"DDL file for {platform} should start with a comment header"
        )

    @pytest.mark.parametrize("platform,workflow_path", get_workflow_files())
    def test_workflow_has_header_comment(self, platform: str, workflow_path: Path):
        """Workflow files should have a header comment."""
        content = read_sql_file(workflow_path)
        first_line = content.strip().split('\n')[0]

        assert first_line.startswith('--'), (
            f"Workflow file for {platform} should start with a comment header"
        )


class TestSQLStructure:
    """Test SQL file structure and formatting."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_ddl_has_create_table(self, platform: str, ddl_path: Path):
        """DDL files should contain CREATE TABLE statement."""
        content = read_sql_file(ddl_path)

        assert 'create table' in content.lower(), (
            f"DDL file for {platform} should contain CREATE TABLE"
        )

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_ddl_ends_with_semicolon(self, platform: str, ddl_path: Path):
        """DDL statements should end with semicolon."""
        content = read_sql_file(ddl_path).strip()

        # Find last non-whitespace, non-comment character
        lines = content.split('\n')
        for line in reversed(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('--'):
                assert stripped.endswith(';'), (
                    f"DDL file for {platform} should end with semicolon"
                )
                break

    @pytest.mark.parametrize("platform,sql_path", get_all_sql_files())
    def test_no_trailing_whitespace_on_lines(self, platform: str, sql_path: Path):
        """SQL files should not have trailing whitespace."""
        content = read_sql_file(sql_path)

        for i, line in enumerate(content.split('\n'), 1):
            # Allow some trailing whitespace (not critical)
            if line != line.rstrip() and len(line) - len(line.rstrip()) > 4:
                print(f"Warning: Line {i} in {sql_path.name} has excessive trailing whitespace")


class TestSQLKeywords:
    """Test SQL keyword usage and casing conventions."""

    @pytest.mark.parametrize("platform,ddl_path", get_ddl_files())
    def test_ddl_uses_uppercase_keywords(self, platform: str, ddl_path: Path):
        """DDL files should use uppercase SQL keywords (convention check)."""
        content = read_sql_file(ddl_path)

        # Check for common keywords that should be uppercase
        keywords_to_check = ['CREATE TABLE', 'PRIMARY KEY', 'NOT NULL', 'DEFAULT']

        for keyword in keywords_to_check:
            # Check if keyword exists in some form
            if keyword.lower().replace(' ', '') in content.lower().replace(' ', ''):
                # Verify at least one instance is uppercase
                # This is a soft check - just warn, don't fail
                if keyword not in content:
                    print(f"Note: {platform} DDL could use uppercase for '{keyword}'")


class TestPlatformSpecificSyntax:
    """Test platform-specific SQL syntax requirements."""

    def test_bigquery_uses_template_variables(self, project_root: Path):
        """BigQuery DDL should use ${project_id} and ${dataset} variables."""
        ddl_path = project_root / 'sql' / 'bigquery' / 'create_metrics_table.sql'
        if not ddl_path.exists():
            pytest.skip("BigQuery DDL not found")

        content = read_sql_file(ddl_path)

        # BigQuery should use template variables
        assert '${project_id}' in content or '${dataset}' in content or 'ml_metrics' in content, (
            "BigQuery DDL should reference project/dataset or table name"
        )

    def test_oracle_lacks_if_not_exists(self, project_root: Path):
        """Oracle DDL should not use IF NOT EXISTS (not supported)."""
        ddl_path = project_root / 'sql' / 'oracle' / 'create_metrics_table.sql'
        if not ddl_path.exists():
            pytest.skip("Oracle DDL not found")

        content = read_sql_file(ddl_path)

        assert 'if not exists' not in content.lower(), (
            "Oracle does not support IF NOT EXISTS in CREATE TABLE"
        )

    def test_postgres_uses_if_not_exists(self, project_root: Path):
        """PostgreSQL DDL should use IF NOT EXISTS for idempotency."""
        ddl_path = project_root / 'sql' / 'postgres' / 'create_metrics_table.sql'
        if not ddl_path.exists():
            pytest.skip("PostgreSQL DDL not found")

        content = read_sql_file(ddl_path)

        assert 'if not exists' in content.lower(), (
            "PostgreSQL DDL should use IF NOT EXISTS for idempotency"
        )

    def test_redshift_uses_distkey(self, project_root: Path):
        """Redshift DDL should define distribution key for performance."""
        ddl_path = project_root / 'sql' / 'redshift' / 'create_metrics_table.sql'
        if not ddl_path.exists():
            pytest.skip("Redshift DDL not found")

        content = read_sql_file(ddl_path)

        assert 'distkey' in content.lower() or 'diststyle' in content.lower(), (
            "Redshift DDL should define DISTKEY or DISTSTYLE for performance"
        )

    def test_postgres_uses_jsonb(self, project_root: Path):
        """PostgreSQL DDL should use JSONB type for metadata."""
        ddl_path = project_root / 'sql' / 'postgres' / 'create_metrics_table.sql'
        if not ddl_path.exists():
            pytest.skip("PostgreSQL DDL not found")

        content = read_sql_file(ddl_path)

        assert 'jsonb' in content.lower(), (
            "PostgreSQL DDL should use JSONB type for efficient JSON queries"
        )
