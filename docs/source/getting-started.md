# Getting Started

This guide will help you get up and running with datamaestro quickly.

## Installation

Install the core package:

```bash
pip install datamaestro
```

Install domain-specific plugins as needed:

```bash
pip install datamaestro-text   # NLP datasets
pip install datamaestro-image  # Image datasets
pip install datamaestro-ml     # ML datasets
```

## Basic Usage

### Finding Datasets

Use the `search` command to find datasets:

```bash
# Search by name
datamaestro search mnist

# Search by tag
datamaestro search tag:classification

# Search by task
datamaestro search task:image-classification

# Search in a specific repository
datamaestro search repo:image mnist

# Combine search terms (AND)
datamaestro search mnist tag:benchmark
```

### Getting Dataset Information

```bash
datamaestro info com.lecun.mnist
```

Output:
```
com.lecun.mnist
http://yann.lecun.com/exdb/mnist/
Types (ids): datamaestro_image.data.ImageClassification
Tags: benchmark, classification
Tasks: image-classification

The MNIST database of handwritten digits...
```

### Downloading Datasets

#### Command Line

```bash
# Download only (no preparation)
datamaestro download com.lecun.mnist

# Download and prepare (returns JSON)
datamaestro prepare com.lecun.mnist
```

#### Python API

Use {py:func}`~datamaestro.context.prepare_dataset` to download and access datasets:

```python
from datamaestro import prepare_dataset

# Download and prepare the dataset
ds = prepare_dataset("com.lecun.mnist")

# Access training data
train_images = ds.train.images.data()  # numpy array
train_labels = ds.train.labels.data()  # numpy array

print(f"Training samples: {train_images.shape[0]}")
print(f"Image shape: {train_images.shape[1:]}")
```

## Working with Different Data Types

### CSV Data

```python
from datamaestro import prepare_dataset

ds = prepare_dataset("some.csv.dataset")

# Get the file path
csv_path = ds.path

# Or use pandas integration if available
import pandas as pd
df = pd.read_csv(ds.path)
```

### Tensor Data (IDX format)

The {py:class}`~datamaestro.data.tensor.IDX` type handles MNIST-style IDX files:

```python
ds = prepare_dataset("com.lecun.mnist")

# IDX files are automatically parsed to numpy arrays
images = ds.train.images.data()  # Returns numpy array
labels = ds.train.labels.data()  # Returns numpy array
```

### HuggingFace Datasets

Some datasets integrate with HuggingFace:

```python
ds = prepare_dataset("some.huggingface.dataset")

# Access the HuggingFace dataset object
hf_dataset = ds.dataset
```

## Using with Experimaestro

Datamaestro integrates seamlessly with [experimaestro](http://experimaestro.github.io/experimaestro-python/) for experiment management:

```python
from experimaestro import experiment
from datamaestro import prepare_dataset

@experiment()
def my_experiment():
    # Datasets are automatically tracked
    ds = prepare_dataset("com.lecun.mnist")

    # Use in your experiment
    train_model(ds.train)
```

## Data Storage Location

By default, datasets are stored in `~/datamaestro/data/`. You can change this:

### Environment Variable

```bash
export DATAMAESTRO_DIR=/path/to/data
datamaestro prepare com.lecun.mnist
```

### Command Line Option

```bash
datamaestro --data /path/to/data prepare com.lecun.mnist
```

### Python API

```python
from pathlib import Path
from datamaestro import prepare_dataset

ds = prepare_dataset("com.lecun.mnist", context=Path("/path/to/data"))
```

## Next Steps

- [Dataset Definition](datasets.rst): Learn how to define your own datasets
- [CLI Reference](cli.md): Complete command line reference
- [Configuration](configuration.md): Advanced configuration options
- [API Reference](api/index.md): Full API documentation
