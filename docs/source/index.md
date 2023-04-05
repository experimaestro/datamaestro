# Datamaestro

```{toctree}
---
maxdepth: 1
caption: "Contents:"
---
developping
datasets
api/index
```


This projects aims at grouping utilities to deal with the numerous and heterogenous datasets present on the Web. It aims
at being

1. a reference for available resources, listing datasets
1. a tool to automatically download and process resources (when freely available)
1. integration with the [experimaestro](http://experimaestro.github.io/experimaestro-python/) experiment manager.
1. (planned) a tool that allows to copy data from one computer to another

Each datasets is uniquely identified by a qualified name such as `com.lecun.mnist`, which is usually the inversed path to the domain name of the website associated with the dataset.

The main repository only deals with very generic processing (downloading, basic pre-processing and data types). Plugins can then be registered that provide access to domain specific datasets.


## List of repositories

- [NLP and information access related dataset](https://github.com/experimaestro/datamaestro_text)
- [image-related dataset](https://github.com/experimaestro/datamaestro_image)
- [machine learning](https://github.com/experimaestro/datamaestro_ml) contains standard ML datasets

# Detailed example

## Python definition of datasets

Each dataset (or a set of related datasets) is described in Python using a mix of declarative
and imperative statements. Its syntax is described in the [documentation](https://datamaestro.readthedocs.io).
For MNIST, this gives

```python
from datamaestro_image.data import ImageClassification, LabelledImages, Base
from datamaestro.data.ml import Supervised
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

  The MNIST database of handwritten digits, available from this page, has a
  training set of 60,000 examples, and a test set of 10,000 examples. It is a
  subset of a larger set available from NIST. The digits have been
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

## Retrieve and download

The commmand line interface allows to download automatically the different resources. Datamaestro extensions can provide additional processing tools.

```bash
$ datamaestro search mnist
com.lecun.mnist

$ datamaestro prepare com.lecun.mnist
INFO:root:Downloading http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz into /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/t10k-labels-idx1-ubyte
INFO:root:Transforming file
INFO:root:Created file /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/t10k-labels-idx1-ubyte
INFO:root:Downloading http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz into /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/t10k-images-idx3-ubyte
INFO:root:Transforming file
INFO:root:Created file /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/t10k-images-idx3-ubyte
INFO:root:Downloading http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz into /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/train-labels-idx1-ubyte
INFO:root:Downloading http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz
Downloading http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz: 32.8kB [00:00, 92.1kB/s]
INFO:root:Transforming file
INFO:root:Created file /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/train-labels-idx1-ubyte
INFO:root:Downloading http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz into /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/train-images-idx3-ubyte
INFO:root:Downloading http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz
Downloading http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz: 9.92MB [00:00, 10.6MB/s]
INFO:root:Transforming file
INFO:root:Created file /home/bpiwowar/datamaestro/data/image/com/lecun/mnist/train-images-idx3-ubyte
...JSON...
```

The previous command also returns a JSON on standard output
```json
{
  "train": {
    "images": {
      "path": "/data/bpiwowar/datamaestro/data/image/com/lecun/mnist/train-images-idx3-ubyte"
    },
    "labels": {
      "path": "/data/bpiwowar/datamaestro/data/image/com/lecun/mnist/train-labels-idx1-ubyte"
    }
  },
  "test": {
    "images": {
      "path": "/data/bpiwowar/datamaestro/data/image/com/lecun/mnist/t10k-images-idx3-ubyte"
    },
    "labels": {
      "path": "/data/bpiwowar/datamaestro/data/image/com/lecun/mnist/t10k-labels-idx1-ubyte"
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
