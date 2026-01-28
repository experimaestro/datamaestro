# Configuration

Datamaestro uses a hierarchical configuration system with global settings, user preferences, and per-invocation options.

## Data Directory

The main data directory stores all downloaded datasets. Default location: `~/datamaestro/`

### Setting the Data Directory

**Environment variable (recommended for system-wide configuration):**

```bash
export DATAMAESTRO_DIR=/path/to/data
```

**Command line option:**

```bash
datamaestro --data /path/to/data prepare com.lecun.mnist
```

**Python API:**

```python
from pathlib import Path
from datamaestro import prepare_dataset

ds = prepare_dataset("com.lecun.mnist", context=Path("/path/to/data"))
```

## Directory Structure

```
~/datamaestro/
├── data/                    # Downloaded datasets
│   ├── image/              # Image repository datasets
│   │   └── com/lecun/mnist/
│   ├── text/               # Text repository datasets
│   └── ml/                 # ML repository datasets
├── cache/                   # Temporary download cache
└── settings.json           # Global settings
```

## Settings Files

### Global Settings

Located at `$DATAMAESTRO_DIR/settings.json`:

```json
{
  "datafolders": {
    "large_data": "/mnt/storage/datasets",
    "local_cache": "/tmp/datamaestro"
  }
}
```

### User Settings

Located at `~/.config/datamaestro/user.json`:

```json
{
  "default_repository": "text"
}
```

## Data Folders

Data folders allow datasets to reference pre-existing data locations without copying files.

### Configuring Data Folders

**Command line:**

```bash
# Set a data folder
datamaestro datafolders set my_data /path/to/existing/data

# List configured folders
datamaestro datafolders list
```

**In settings.json:**

```json
{
  "datafolders": {
    "my_data": "/path/to/existing/data"
  }
}
```

### Using Data Folders in Dataset Definitions

Use {py:class}`~datamaestro.context.DatafolderPath` to reference configured folders:

```python
from datamaestro.context import DatafolderPath
from datamaestro.definitions import dataset

@dataset(MyDataType)
def my_dataset():
    # Reference a file in a configured data folder
    path = DatafolderPath("my_data", "subdir/file.csv")
    return MyDataType(path=path)
```

## Context API

The {py:class}`~datamaestro.context.Context` class manages all configuration state:

```python
from datamaestro.context import Context

# Get the singleton context instance
ctx = Context.instance()

# Access paths
print(ctx.datapath)    # ~/datamaestro/data
print(ctx.cachepath)   # ~/datamaestro/cache

# Access settings
print(ctx.settings.datafolders)

# Iterate over repositories
for repo in ctx.repositories():
    print(repo.id, repo.name)

# Find a dataset
ds = ctx.dataset("com.lecun.mnist")
```

## Caching

Downloaded files are cached in `$DATAMAESTRO_DIR/cache/` to avoid re-downloading.

### Cache Behavior

- Files are identified by URL hash
- Cache is checked before downloading
- Use `--keep-downloads` to preserve archive files after extraction

### Clearing the Cache

```bash
rm -rf ~/datamaestro/cache/*
```

## Remote Execution

Datamaestro supports remote execution via experimaestro's rpyc integration:

```bash
datamaestro --host remote-server --pythonpath /usr/bin/python3 prepare com.lecun.mnist
```

This connects to the remote host and executes datamaestro there, useful for:
- Downloading data directly to a compute cluster
- Accessing datasets on remote storage

## Repository Registration

Repositories are registered via Python entry points in `pyproject.toml`:

```toml
[project.entry-points."datamaestro.repositories"]
myrepo = "mypackage:MyRepository"
```

The repository class must inherit from `Repository`:

```python
from datamaestro.context import Repository

class MyRepository(Repository):
    NAMESPACE = "myrepo"
    DESCRIPTION = "My custom datasets"
```
