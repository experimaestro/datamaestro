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
3. **Experimaestro Integration**: Seamless integration with the [experimaestro](http://experimaestro.github.io/experimaestro-python/) experiment manager
4. **Extensible Architecture**: Plugin system for domain-specific datasets

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

Each dataset is described in Python using decorators that combine declarative metadata with imperative data processing. Here's the MNIST example:

```python
from datamaestro_image.data import ImageClassification, LabelledImages
from datamaestro.data.tensor import IDX
from datamaestro.download.single import filedownloader
from datamaestro.definitions import dataset


@filedownloader("train_images.idx", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz")
@filedownloader("train_labels.idx", "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz")
@filedownloader("test_images.idx", "http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz")
@filedownloader("test_labels.idx", "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz")
@dataset(
  ImageClassification,
  url="http://yann.lecun.com/exdb/mnist/",
)
def MNIST(train_images, train_labels, test_images, test_labels):
  """The MNIST database

  The MNIST database of handwritten digits has a training set of 60,000
  examples, and a test set of 10,000 examples. The digits have been
  size-normalized and centered in a fixed-size image.
  """
  return {
    "train": LabelledImages(
      images=IDX(path=train_images),
      labels=IDX(path=train_labels)
    ),
    "test": LabelledImages(
      images=IDX(path=test_images),
      labels=IDX(path=test_labels)
    ),
  }
```

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
- **Data Types**: Structured representations of data (CSV, tensors, etc.) - see {py:class}`~datamaestro.data.Base`
- **Download Handlers**: Decorators that specify how to fetch resources - see [Download Decorators](api/download.rst)
- **Context**: Global configuration for data paths and settings - see {py:class}`~datamaestro.context.Context`

See the documentation sections for detailed information on each concept.
