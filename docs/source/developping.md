# Development Guide

This guide covers how to contribute to datamaestro or develop dataset plugins.

## Setting Up the Development Environment

### Core Library

```bash
# Clone the repository
git clone https://github.com/experimaestro/datamaestro.git
cd datamaestro

# Install in development mode
pip install -e .

# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Plugin Development

To develop a plugin (e.g., `datamaestro_text`):

```bash
# Clone the plugin repository
git clone https://github.com/experimaestro/datamaestro_text.git
cd datamaestro_text

# Install both core and plugin in development mode
pip install -e ../datamaestro
pip install -e .
```

## Code Quality

### Formatting

Code is formatted with [black](https://black.readthedocs.io/) with a maximum line length of 80 characters:

```bash
# Format all files
pre-commit run black --all-files

# Or run black directly
black --line-length 80 src/
```

### Linting

Code is linted with [flake8](https://flake8.pycqa.org/):

```bash
# Lint all files
pre-commit run flake8 --all-files

# Or run flake8 directly
flake8 src/
```

Plugins used:
- `flake8-print` - Warns about print statements
- `flake8-fixme` - Warns about TODO/FIXME comments

### Pre-commit Hooks

All pre-commit hooks:

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run hooks on staged files only
pre-commit run
```

### Commit Messages

We use [conventional commits](https://www.conventionalcommits.org/):

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, missing semicolons, etc.
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests
- `chore`: Maintenance tasks

Examples:
```
feat(download): add support for HuggingFace datasets
fix(archive): handle corrupted zip files gracefully
docs(readme): update installation instructions
```

## Running Tests

```bash
# Run all tests
pytest

# Run a specific test file
pytest src/datamaestro/test/test_record.py

# Run a specific test
pytest src/datamaestro/test/test_record.py -k test_name

# Run with coverage
pytest --cov=datamaestro

# Run with verbose output
pytest -v
```

### Test Fixtures

The `conftest.py` provides useful fixtures:

```python
def test_with_context(context):
    """Test using temporary datamaestro directory"""
    # context is a Context instance with temp directory
    ds = context.dataset("com.example.test")
    ...
```

### Testing Downloads

By default, tests skip actual downloads. To test with real downloads:

```bash
pytest --datamaestro-download
```

## Project Structure

```
datamaestro/
├── src/datamaestro/
│   ├── __init__.py          # Package init, version
│   ├── __main__.py          # CLI entry point
│   ├── context.py           # Context and Repository classes
│   ├── definitions.py       # Dataset decorators and models
│   ├── record.py            # Record system (deprecated)
│   ├── search.py            # Search conditions
│   ├── settings.py          # Settings management
│   ├── utils.py             # Utility functions
│   ├── sphinx.py            # Sphinx documentation extension
│   ├── data/                # Data type definitions
│   │   ├── __init__.py      # Base data types
│   │   ├── csv.py           # CSV data types
│   │   ├── tensor.py        # Tensor data types (IDX)
│   │   ├── ml.py            # ML data types
│   │   └── huggingface.py   # HuggingFace integration
│   ├── download/            # Download handlers
│   │   ├── __init__.py      # Base Download class
│   │   ├── single.py        # Single file downloads
│   │   ├── archive.py       # Archive extraction
│   │   ├── multiple.py      # Multiple file downloads
│   │   ├── links.py         # Dataset links
│   │   ├── huggingface.py   # HuggingFace downloads
│   │   └── wayback.py       # Internet Archive
│   ├── test/                # Tests
│   └── templates/           # Dataset templates
├── docs/                    # Sphinx documentation
└── pyproject.toml           # Project configuration
```

## Creating a New Repository Plugin

### 1. Project Structure

```
datamaestro_myplugin/
├── src/datamaestro_myplugin/
│   ├── __init__.py          # Repository class
│   ├── config/              # Dataset definitions
│   │   └── com/
│   │       └── example.py   # com.example.* datasets
│   └── data/                # Data type definitions
│       └── __init__.py
├── pyproject.toml
└── README.md
```

### 2. Repository Class

```python
# src/datamaestro_myplugin/__init__.py
from datamaestro.context import Repository

class MyPluginRepository(Repository):
    NAMESPACE = "myplugin"
    DESCRIPTION = "My custom datasets"
```

### 3. Entry Point Registration

```toml
# pyproject.toml
[project.entry-points."datamaestro.repositories"]
myplugin = "datamaestro_myplugin:MyPluginRepository"
```

### 4. Dataset Definition

```python
# src/datamaestro_myplugin/config/com/example.py
from datamaestro.definitions import dataset
from datamaestro.download.single import filedownloader
from datamaestro.data import Base

@filedownloader("data.csv", "http://example.com/data.csv")
@dataset(Base, url="http://example.com")
def my_dataset(data):
    """My example dataset

    Description of the dataset.
    """
    return Base(path=data)
```

## Adding a New Download Handler

```python
# src/datamaestro/download/myhandler.py
from datamaestro.download import Download
from datamaestro.definitions import DatasetAnnotation

class MyDownload(Download):
    def __init__(self, varname: str, url: str):
        super().__init__(varname)
        self.url = url

    def prepare(self):
        """Prepare the resource and return the path/data"""
        # Download logic here
        path = self.download_file(self.url)
        return path

    def download(self, force=False):
        """Download the resource"""
        # Actual download implementation
        ...

def mydownloader(varname: str, url: str):
    """Decorator for my custom download handler"""
    def decorator(dataset):
        download = MyDownload(varname, url)
        download.register(dataset)
        return dataset
    return decorator
```

## Documentation

### Building Documentation

```bash
cd docs
make html
```

Output is in `docs/build/html/`.

### Sphinx Extensions

Datamaestro provides a custom Sphinx extension for documenting datasets:

```rst
.. dm:repository:: text

.. dm:datasets::
```

This automatically generates documentation from registered datasets.

## Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create a git tag: `git tag v1.2.3`
4. Push with tags: `git push --tags`
5. GitHub Actions will build and publish to PyPI

## Troubleshooting

### Import Errors

If you get import errors after installing in development mode:

```bash
pip install -e . --force-reinstall
```

### Pre-commit Hook Failures

If pre-commit hooks fail:

```bash
# Update hooks
pre-commit autoupdate

# Clear cache
pre-commit clean
```

### Test Discovery Issues

Ensure test files are named `test_*.py` and are in the `src/datamaestro/test/` directory.
