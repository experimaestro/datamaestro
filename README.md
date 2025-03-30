[![PyPI version](https://badge.fury.io/py/datamaestro.svg)](https://badge.fury.io/py/datamaestro) [![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit) [![DOI](https://zenodo.org/badge/4573876.svg)](https://zenodo.org/badge/latestdoi/4573876)



# Introduction

Full documentation can be found at http://datamaestro.rtfd.io

This projects aims at grouping utilities to deal with the numerous and heterogenous datasets present on the Web. It aims
at being

1. a reference for available resources, listing datasets
1. a tool to automatically download and process resources (when freely available)
1. integration with the [experimaestro](http://experimaestro-python.rtfd.io/) experiment manager.
1. (planned) a tool that allows to copy data from one computer to another

Each datasets is uniquely identified by a qualified name such as `com.lecun.mnist`, which is usually the inversed path to the domain name of the website associated with the dataset.

The main repository only deals with very generic processing (downloading, basic pre-processing and data types). Plugins can then be registered that provide access to domain specific datasets.



## List of repositories

- [Information Retrieval](https://github.com/bpiwowar/experimaestro-ir) [![PyPI version](https://badge.fury.io/py/experimaestro-ir.svg)](https://badge.fury.io/py/experimaestro-ir)

- [NLP and information access related dataset](https://github.com/experimaestro/datamaestro_text) [![PyPI version](https://badge.fury.io/py/datamaestro-text.svg)](https://badge.fury.io/py/datamaestro-text) \
  Natural Language Processing (e.g. Sentiment101) and Information access (e.g. TREC) datasets
- [image-related dataset](https://github.com/experimaestro/datamaestro_image) [![PyPI version](https://badge.fury.io/py/datamaestro-image.svg)](https://badge.fury.io/py/datamaestro-image)
  Image related datasets (e.g. MNIST)

- [machine learning](https://github.com/experimaestro/datamaestro_ml) [![PyPI version](https://badge.fury.io/py/datamaestro-ml.svg)](https://badge.fury.io/py/datamaestro-ml)\
 Generic machine learning datasets


# Command line interface (CLI)


The command line interface allows to interact with the datasets. The commands are listed below, help can be found by typing `datamaestro COMMAND --help`:

- `search` search dataset by name, tags and/or tasks
- `download` download files (if accessible on Internet) or ask for download path otherwise
- `prepare` download dataset files and outputs a JSON containing path and other dataset information
- `repositories` list the available repositories
- `orphans` list data directories that do no correspond to any registered dataset (and allows to clean them up)
- `create-dataset` creates a dataset definition


# Example (CLI)

## Retrieve and download

The commmand line interface allows to download automatically the different resources. Datamaestro extensions can provide additional processing tools.

```bash
$ datamaestro search tag:image
[image] com.lecun.mnist

$ datamaestro prepare com.lecun.mnist
INFO:root:Materializing 4 resources
INFO:root:Downloading https://ossci-datasets.s3.amazonaws.com/mnist/train-images-idx3-ubyte.gz into .../datamaestro/store/com/lecun/train_images.idx
INFO:root:Downloading https://ossci-datasets.s3.amazonaws.com/mnist/t10k-images-idx3-ubyte.gz into .../datamaestro/store/com/lecun/test_images.idx
INFO:root:Downloading https://ossci-datasets.s3.amazonaws.com/mnist/t10k-labels-idx1-ubyte.gz into .../datamaestro/store/com/lecun/test_labels.idx
```

The previous command also returns a JSON on standard output
```json
{
  "train": {
    "images": {
      "path": ".../data/image/com/lecun/mnist/train_images.idx"
    },
    "labels": {
      "path": ".../data/image/com/lecun/mnist/train_labels.idx"
    }
  },
  "test": {
    "images": {
      "path": ".../data/image/com/lecun/mnist/test_images.idx"
    },
    "labels": {
      "path": ".../data/image/com/lecun/mnist/test_labels.idx"
    }
  },
  "id": "com.lecun.mnist"
}
```

For those using Python, this is even better since the IDX format is supported

```python
In [1]: from datamaestro import prepare_dataset
In [2]: ds = prepare_dataset("com.lecun.mnist")
In [3]: ds.train.images.data().dtype, ds.train.images.data().shape
Out[3]: (dtype('uint8'), (60000, 28, 28))
```


## Python definition of datasets

Each dataset (or a set of related datasets) is described in Python using a mix of declarative
and imperative statements. This allows to quickly define how to download dataset using the
datamaestro declarative API; the imperative part is used when creating the JSON output,
and is integrated with [experimaestro](http://experimaestro.github.io/experimaestro-python).

Its syntax is described in the [documentation](https://datamaestro.readthedocs.io).


For instance, the MNIST dataset can be described by the following

```python
from datamaestro import dataset
from datamaestro.download.single import download_file
from datamaestro_image.data import ImageClassification, LabelledImages, IDXImage


@filedownloader("train_images.idx", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz")
@filedownloader("train_labels.idx", "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz")
@filedownloader("test_images.idx", "http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz")
@filedownloader("test_labels.idx", "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz")
@dataset(
  ImageClassification,
  url="http://yann.lecun.com/exdb/mnist/",
)

    return ImageClassification(
        train=LabelledImages(
            images=IDXImage(path=train_images), labels=IDXImage(path=train_labels)
        ),
        test=LabelledImages(
            images=IDXImage(path=test_images), labels=IDXImage(path=test_labels)
        ),
    )
```

When building dataset modules, some extra documentation can be provided:

```yaml
  ids: [com.lecun.mnist]
  entry_point: "datamaestro_image.config.com.lecun:mnist"
  title: The MNIST database
  url: http://yann.lecun.com/exdb/mnist/
  groups: [image-classification]
  description: |
    The MNIST database of handwritten digits, available from this page,
    has a training set of 60,000 examples, and a test set of 10,000
    examples. It is a subset of a larger set available from NIST. The
    digits have been size-normalized and centered in a fixed-size image.
```

This will allow to

1. Document the dataset
2. Allow to use the command line interface to manipulate it (download resources, etc.)
