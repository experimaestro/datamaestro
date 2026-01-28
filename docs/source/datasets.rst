Dataset Definition
==================

A dataset definition in datamaestro combines declarative metadata with imperative
data processing logic. This page explains how to create your own dataset definitions.

Components of a Dataset
-----------------------

Every dataset definition includes:

1. **ID**: Unique identifier determined by module location and function name
2. **Meta-information**: Tags, tasks, URL, description
3. **Download specification**: What resources to fetch
4. **Data access**: How to structure the data in Python

Basic Example
=============

Here's the complete MNIST example from `datamaestro_image`:

.. code-block:: python
    :caption: File: ``datamaestro_image/config/com/lecun.py``

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

Dataset ID Naming
-----------------

The dataset ID is derived from:

1. **Module path**: ``datamaestro_image.config.com.lecun`` → ``com.lecun``
2. **Function name**: ``MNIST`` → ``.mnist`` (lowercased)
3. **Final ID**: ``com.lecun.mnist``

The convention follows reversed domain names (like Java packages):

- ``com.lecun.mnist`` for http://yann.lecun.com/exdb/mnist/
- ``org.trec.robust04`` for https://trec.nist.gov/ ROBUST04 track
- ``io.huggingface.squad`` for HuggingFace datasets

The `@dataset` Annotation
=========================

The ``@dataset`` decorator is the main annotation for defining datasets.

.. autoclass:: datamaestro.definitions.dataset

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Parameter
     - Description
   * - ``base``
     - The base data type class (e.g., ``ImageClassification``). Can be inferred from return type annotation.
   * - ``id``
     - Override the automatic ID. Use ``"."`` prefix to replace only the last component.
   * - ``url``
     - URL to the dataset's homepage.
   * - ``doi``
     - DOI of the associated paper.
   * - ``timestamp``
     - Version timestamp for evolving datasets.
   * - ``size``
     - Dataset size (for documentation).
   * - ``as_prepare``
     - If True, the function receives the dataset object for manual resource handling.

ID Override Examples
--------------------

.. code-block:: python

    # Full ID override
    @dataset(MyType, id="org.example.custom")
    def ignored_name():
        ...

    # Replace last component only (in module com.example)
    @dataset(MyType, id=".v2")  # Results in com.example.v2
    def original():
        ...

    # Empty string uses module path only
    @dataset(MyType, id="")  # Results in com.example (no function name)
    def main():
        ...

Download Decorators
===================

Download decorators specify how to fetch resources. They pass file paths as
arguments to the dataset function. See the :doc:`api/download` for full reference.

Single Files
------------

Use :py:func:`~datamaestro.download.single.filedownloader` for single file downloads:

.. code-block:: python

    from datamaestro.download.single import filedownloader

    @filedownloader("data.csv", "http://example.com/data.csv")
    @dataset(CSVData)
    def my_dataset(data):
        return CSVData(path=data)

The :py:func:`~datamaestro.download.single.filedownloader` decorator:

- Downloads the URL to the dataset's data directory
- Automatically decompresses ``.gz``, ``.bz2`` files
- Passes a :py:class:`pathlib.Path` to the function

Archives
--------

Use :py:func:`~datamaestro.download.archive.zipdownloader` or
:py:func:`~datamaestro.download.archive.tardownloader` for archives:

.. code-block:: python

    from datamaestro.download.archive import zipdownloader, tardownloader

    @zipdownloader("data", "http://example.com/archive.zip")
    @dataset(MyData)
    def zipped_dataset(data):
        # data is a Path to the extracted directory
        return MyData(path=data / "file.csv")

    @tardownloader("data", "http://example.com/archive.tar.gz",
                   subpath="archive/subdir")  # Extract only a subdirectory
    @dataset(MyData)
    def tar_dataset(data):
        return MyData(path=data / "file.csv")

Multiple Files
--------------

.. code-block:: python

    from datamaestro.download.multiple import MultipleFileDownloader

    @MultipleFileDownloader(
        "files",
        "http://example.com/part1.csv",
        "http://example.com/part2.csv",
        "http://example.com/part3.csv",
    )
    @dataset(MyData)
    def multi_file_dataset(files):
        # files is a list of Paths
        return MyData(paths=files)

HuggingFace Integration
-----------------------

.. code-block:: python

    from datamaestro.download.huggingface import HuggingFaceDownloader

    @HuggingFaceDownloader("dataset", "squad")
    @dataset(QADataset)
    def squad(dataset):
        return QADataset(hf_dataset=dataset)

Links to Other Datasets
-----------------------

Use :py:func:`~datamaestro.download.links.links` to reference other datasets:

.. code-block:: python

    from datamaestro.download.links import links

    @links("base", "com.example.base_dataset")
    @dataset(ExtendedData)
    def extended_dataset(base):
        # base is the prepared base dataset
        return ExtendedData(base=base)

Data Types
==========

Data types define the structure of returned data. They inherit from
:py:class:`datamaestro.data.Base` and use experimaestro's configuration system.
See the :doc:`api/data` for full reference.

Built-in Types
--------------

- :py:class:`datamaestro.data.Base` - Base class for all data types
- :py:class:`datamaestro.data.File` - Single file reference
- :py:class:`datamaestro.data.csv.Generic` - CSV file
- :py:class:`datamaestro.data.csv.Matrix` - CSV with numeric data
- :py:class:`datamaestro.data.tensor.IDX` - IDX tensor format (MNIST)
- :py:class:`datamaestro.data.ml.Supervised` - Supervised learning data

Custom Data Types
-----------------

Create custom data types by inheriting from :py:class:`~datamaestro.data.Base`.
Use ``Param`` from experimaestro to define typed parameters:

.. code-block:: python

    from experimaestro import Config, Param
    from datamaestro.data import Base

    class MyCustomData(Base):
        """My custom data type"""
        path: Param[Path]
        """Path to the data file"""

        num_classes: Param[int] = 10
        """Number of classes"""

        def load(self):
            """Load the data"""
            import pandas as pd
            return pd.read_csv(self.path)

Tags and Tasks
==============

Add semantic metadata with :py:func:`~datamaestro.definitions.datatags` and
:py:func:`~datamaestro.definitions.datatasks` decorators:

.. code-block:: python

    from datamaestro.definitions import dataset, datatags, datatasks

    @datatags("benchmark", "classification", "vision")
    @datatasks("image-classification", "digit-recognition")
    @dataset(ImageClassification, url="http://example.com")
    def MNIST(train_images, train_labels, test_images, test_labels):
        """Dataset description"""
        return {"train": ..., "test": ...}

Tags and tasks are searchable via the CLI:

.. code-block:: bash

    datamaestro search tag:benchmark
    datamaestro search task:classification

Metadatasets
============

Use :py:class:`~datamaestro.definitions.metadataset` to share common metadata
across related datasets:

.. code-block:: python

    from datamaestro.definitions import metadataset, dataset

    @datatags("trec", "information-retrieval")
    @metadataset(IRDataset)
    class TRECBase:
        """Common base for TREC datasets"""
        pass

    @dataset(TRECBase, url="https://trec.nist.gov/...")
    def robust04():
        ...

    @dataset(TRECBase, url="https://trec.nist.gov/...")
    def robust05():
        ...

Class-based Datasets
====================

For complex datasets, use class-based definitions:

.. code-block:: python

    from datamaestro.data import Base
    from experimaestro import Param

    @dataset(url="http://example.com")
    class ComplexDataset(Base):
        """A complex dataset with multiple configurations"""

        version: Param[str] = "v1"
        split: Param[str] = "train"

        @classmethod
        def __create_dataset__(cls, dataset_def):
            # Custom preparation logic
            return cls(version="v1", split="train")

File Validation
===============

Use :py:class:`~datamaestro.utils.HashCheck` to validate downloaded files with checksums:

.. code-block:: python

    from datamaestro.utils import HashCheck

    @filedownloader(
        "data.csv",
        "http://example.com/data.csv",
        checker=HashCheck("sha256", "abc123...")
    )
    @dataset(CSVData)
    def validated_dataset(data):
        return CSVData(path=data)

Testing Datasets
================

Test your dataset definitions:

.. code-block:: python

    def test_my_dataset():
        from datamaestro import prepare_dataset

        ds = prepare_dataset("com.example.mydataset")
        assert ds.train is not None
        assert ds.test is not None

Use ``pytest`` with the ``--datamaestro-download`` flag to actually download
during tests (otherwise downloads are skipped).
