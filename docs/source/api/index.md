# API Reference

This section documents the datamaestro Python API. See also:

- [Data Types](data.md) - Dataset content structures
- [Download Decorators](download.rst) - Resource fetching
- [Records](records.rst) - Heterogeneous containers (deprecated)

## Core Functions

### prepare_dataset

The main entry point for using datasets in Python:

```python
from datamaestro import prepare_dataset

# By dataset ID
ds = prepare_dataset("com.lecun.mnist")

# With custom data directory
from pathlib import Path
ds = prepare_dataset("com.lecun.mnist", context=Path("/custom/path"))
```

```{eval-rst}
.. autofunction:: datamaestro.context.prepare_dataset
```

### find_dataset

Find a dataset without downloading:

```python
from datamaestro import find_dataset

ds = find_dataset("com.lecun.mnist")
print(ds.url)  # Dataset URL
print(ds.description)  # Dataset description
```

```{eval-rst}
.. autofunction:: datamaestro.context.find_dataset
```

### get_dataset

Get a dataset without downloading (assumes already downloaded):

```python
from datamaestro import get_dataset

ds = get_dataset("com.lecun.mnist")
```

```{eval-rst}
.. autofunction:: datamaestro.context.get_dataset
```

## Context

The `Context` class manages global state:

```python
from datamaestro.context import Context

ctx = Context.instance()

# Access paths
ctx.datapath      # ~/datamaestro/data
ctx.cachepath     # ~/datamaestro/cache

# Iterate over datasets
for ds in ctx.datasets():
    print(ds.id)

# Get a specific dataset
ds = ctx.dataset("com.lecun.mnist")
```

```{eval-rst}
.. autoclass:: datamaestro.context.Context
    :members: instance, datapath, cachepath, repositories, datasets, dataset
```

## Dataset Classes

### AbstractDataset

Base class for all dataset definitions:

```{eval-rst}
.. autoclass:: datamaestro.definitions.AbstractDataset
    :members: download, prepare
```

### DatasetWrapper

The standard dataset wrapper created by the `@dataset` decorator:

```{eval-rst}
.. autoclass:: datamaestro.definitions.DatasetWrapper
    :members: prepare, download, datapath
```

## Decorators

### @dataset

Main decorator for defining datasets (see [Dataset Definition](../datasets.rst) for details):

```{eval-rst}
.. autoclass:: datamaestro.definitions.dataset
    :noindex:
```

### @metadataset

Decorator for abstract/shared dataset definitions:

```{eval-rst}
.. autoclass:: datamaestro.definitions.metadataset
```

### @datatags / @datatasks

Add semantic metadata:

```python
from datamaestro.definitions import datatags, datatasks

@datatags("benchmark", "classification")
@datatasks("image-classification")
@dataset(MyType)
def my_dataset():
    ...
```

## Repository

```{eval-rst}
.. autoclass:: datamaestro.context.Repository
    :members: datapath, search
```

## Search Conditions

For programmatic dataset search:

```python
from datamaestro.search import Condition, AndCondition, TagCondition

# Parse search term
condition = Condition.parse("tag:classification")

# Build complex queries
query = AndCondition()
query.append(TagCondition("classification"))
query.append(Condition.parse("repo:image"))

# Match datasets
for ds in context.datasets():
    if query.match(ds):
        print(ds.id)
```

```{eval-rst}
.. autoclass:: datamaestro.search.Condition
    :members: parse, match

.. autoclass:: datamaestro.search.AndCondition
    :members: append, match

.. autoclass:: datamaestro.search.TagCondition

.. autoclass:: datamaestro.search.TaskCondition

.. autoclass:: datamaestro.search.RepositoryCondition

.. autoclass:: datamaestro.search.TypeCondition
```
