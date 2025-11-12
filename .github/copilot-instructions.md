# GitHub Copilot Instructions for ml_using-SQL

## Project Overview

This repository is focused on machine learning implementations using SQL. The project supports both Python and JavaScript/TypeScript code.

## Project Structure

- Python files: Machine learning models and SQL integration scripts
- JavaScript/TypeScript files: Related tooling and utilities
- `.github/workflows/`: CI/CD workflows for linting and code quality

## Development Setup

### Python Development

1. **Python Version**: The project supports Python 3.8, 3.9, and 3.10
2. **Install Dependencies**:
   ```bash
   python -m pip install --upgrade pip
   pip install pylint
   # Install any additional dependencies as needed
   ```

### JavaScript Development

1. **Install ESLint**:
   ```bash
   npm install eslint@8.10.0
   npm install @microsoft/eslint-formatter-sarif@3.1.0
   ```

## Linting and Code Quality

### Python Linting

- **Tool**: Pylint
- **Command**: `pylint $(git ls-files '*.py')`
- **Note**: All Python code must pass Pylint checks

### JavaScript Linting

- **Tool**: ESLint
- **Config**: `.eslintrc.js`
- **Command**: `npx eslint . --config .eslintrc.js --ext .js,.jsx,.ts,.tsx`
- **Note**: All JavaScript/TypeScript code must pass ESLint checks

### Code Quality

- The project uses SonarCloud for additional code quality analysis
- Ensure changes maintain or improve code quality metrics

## Coding Conventions

### General Guidelines

- Write clean, maintainable code with appropriate comments
- Follow existing code style and patterns in the repository
- Ensure all linters pass before committing changes

### Python Conventions

- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Write docstrings for functions and classes
- Keep functions focused and modular

### JavaScript/TypeScript Conventions

- Follow ESLint configuration rules
- Use modern JavaScript/TypeScript features appropriately
- Maintain consistent formatting and naming conventions

## Testing

- When adding new features, include appropriate tests
- Ensure existing tests continue to pass
- Test ML models and SQL queries thoroughly

## Making Changes

1. Always run linters before committing
2. Test your changes locally
3. Ensure CI/CD workflows will pass
4. Keep changes focused and minimal
5. Update documentation if needed

## Important Notes

- This is a machine learning project with SQL integration
- Be mindful of data handling and query efficiency
- Consider performance implications of ML model changes
- Ensure SQL queries are optimized and secure
