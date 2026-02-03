# Datamaestro

```{toctree}
---
maxdepth: 2
caption: User Guide
---
getting-started
cli
configuration
```

```{toctree}
---
maxdepth: 2
caption: Dataset Development
---
datasets
developping
```

```{toctree}
---
maxdepth: 2
caption: API Reference
---
api/index
api/data
api/download
api/records
```

## Overview

Datamaestro is a Python framework for managing, organizing, and downloading datasets. It provides:

1. **Dataset Registry**: A reference for available resources with qualified names (e.g., `com.lecun.mnist`)
2. **Automatic Downloads**: Tools to automatically download and preprocess resources when freely available
3. **Resource Pipelines**: A DAG-based resource system with dependency tracking, transient intermediaries, and two-path download safety
4. **Experimaestro Integration**: Seamless integration with the [experimaestro](http://experimaestro.github.io/experimaestro-python/) experiment manager
5. **Extensible Architecture**: Plugin system for domain-specific datasets

Each dataset is uniquely identified by a qualified name derived from the website's domain (e.g., `com.lecun.mnist` for MNIST from yann.lecun.com).

## Installation

```bash
pip install datamaestro
```

For domain-specific datasets, install the corresponding plugins:

```bash
# NLP and information retrieval datasets
pip install datamaestro-text

# Image datasets (MNIST, CIFAR, etc.)
pip install datamaestro-image

# Generic machine learning datasets
pip install datamaestro-ml
```

## Quick Start

### Command Line

```bash
# Search for datasets
datamaestro search mnist

# Get information about a dataset
datamaestro info com.lecun.mnist

# Download and prepare a dataset
datamaestro prepare com.lecun.mnist
```

### Python API

Use {py:func}`~datamaestro.context.prepare_dataset` to download and access datasets:

```python
from datamaestro import prepare_dataset

# Download and get the dataset
ds = prepare_dataset("com.lecun.mnist")

# Access the data
print(ds.train.images.data().shape)  # (60000, 28, 28)
print(ds.test.labels.data().shape)   # (10000,)
```

## Available Repositories

The main datamaestro package provides generic processing capabilities. Domain-specific datasets are provided through plugins:

| Repository | Description | Install |
|------------|-------------|---------|
| [datamaestro_text](https://github.com/experimaestro/datamaestro_text) | NLP and information retrieval datasets | `pip install datamaestro-text` |
| [datamaestro_image](https://github.com/experimaestro/datamaestro_image) | Image datasets (MNIST, CIFAR, etc.) | `pip install datamaestro-image` |
| [datamaestro_ml](https://github.com/experimaestro/datamaestro_ml) | Generic ML datasets | `pip install datamaestro-ml` |

## Detailed Example

### Python Definition of Datasets

Datasets are defined as Python classes with resource attributes that describe how to download and process data:

```python
from datamaestro_image.data import ImageClassification, LabelledImages
from datamaestro.data.tensor import IDX
from datamaestro.download.single import FileDownloader
from datamaestro.definitions import Dataset, dataset


@dataset(url="http://yann.lecun.com/exdb/mnist/")
class MNIST(Dataset):
    """The MNIST database of handwritten digits."""

    TRAIN_IMAGES = FileDownloader(
        "train_images.idx",
        "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz",
    )
    TRAIN_LABELS = FileDownloader(
        "train_labels.idx",
        "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz",
    )
    TEST_IMAGES = FileDownloader(
        "test_images.idx",
        "http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz",
    )
    TEST_LABELS = FileDownloader(
        "test_labels.idx",
        "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz",
    )

    def config(self) -> ImageClassification:
        return ImageClassification.C(
            train=LabelledImages(
                images=IDX(path=self.TRAIN_IMAGES.path),
                labels=IDX(path=self.TRAIN_LABELS.path),
            ),
            test=LabelledImages(
                images=IDX(path=self.TEST_IMAGES.path),
                labels=IDX(path=self.TEST_LABELS.path),
            ),
        )
```

Resources are automatically discovered from class attributes and form a
dependency graph. The framework handles:

- **Two-path downloads**: writes to a temporary path, moves to final path on success
- **State tracking**: resource states (NONE/PARTIAL/COMPLETE) persisted in `.state.json`
- **Transient cleanup**: intermediate files deleted after all dependents complete

### Retrieve and Download

The command line interface downloads resources automatically:

```bash
$ datamaestro search mnist
com.lecun.mnist

$ datamaestro prepare com.lecun.mnist
INFO:root:Downloading http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz
...
```

The `prepare` command outputs JSON with file paths:

```json
{
  "train": {
    "images": {"path": "/home/user/datamaestro/data/image/com/lecun/mnist/train-images"},
    "labels": {"path": "/home/user/datamaestro/data/image/com/lecun/mnist/train-labels"}
  },
  "test": {
    "images": {"path": "/home/user/datamaestro/data/image/com/lecun/mnist/t10k-images"},
    "labels": {"path": "/home/user/datamaestro/data/image/com/lecun/mnist/t10k-labels"}
  },
  "id": "com.lecun.mnist"
}
```

### Using in Python

```python
from datamaestro import prepare_dataset

ds = prepare_dataset("com.lecun.mnist")

# Access numpy arrays directly (for IDX format)
print(ds.train.images.data().dtype)   # uint8
print(ds.train.images.data().shape)   # (60000, 28, 28)
print(ds.train.labels.data().shape)   # (60000,)
```

## Key Concepts

- **Dataset ID**: Qualified name like `com.lecun.mnist` derived from the source URL
- **Repository**: A collection of related datasets (e.g., `datamaestro_image`) - see {py:class}`~datamaestro.context.Repository`
- **Resources**: Steps in a download/processing pipeline (files, archives, links) - see [Download Resources](api/download.rst)
- **Data Types**: Structured representations of data (CSV, tensors, etc.) - see {py:class}`~datamaestro.data.Base`
- **Context**: Global configuration for data paths and settings - see {py:class}`~datamaestro.context.Context`

See the documentation sections for detailed information on each concept.
