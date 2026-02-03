Dataset Definition
==================

A dataset definition in datamaestro combines declarative metadata with imperative
data processing logic. This page explains how to create your own dataset definitions.

Components of a Dataset
-----------------------

Every dataset definition includes:

1. **ID**: Unique identifier determined by module location and class/function name
2. **Meta-information**: Tags, tasks, URL, description
3. **Resources**: What files/data to fetch (defined as class attributes)
4. **Data access**: How to structure the data in Python

Class-based Datasets (Preferred)
================================

The preferred way to define datasets uses class-based definitions where
resources are declared as class attributes. The framework automatically
detects resources and builds a dependency DAG.

Basic Example
-------------

.. code-block:: python
    :caption: File: ``datamaestro_image/config/com/lecun.py``

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

Advantages of class-based definitions:

1. **Explicit pipeline** --- dependencies between resources are visible
2. **Transient intermediaries** --- intermediate files can be deleted after processing
3. **Auto-naming** --- resource names are auto-detected from class attribute names
4. **Two-path safety** --- incomplete downloads never appear at the final path

Resource Pipelines
------------------

Resources can depend on other resources, forming a processing pipeline:

.. code-block:: python

    @dataset(url="http://example.com")
    class ProcessedDataset(MyData):
        # Raw download — deleted after processing completes
        RAW = FileDownloader(
            "raw.gz", "http://example.com/data.gz",
            transient=True,
        )
        # Processed output — kept permanently
        PROCESSED = MyProcessor.from_file(RAW)

        @classmethod
        def __create_dataset__(cls, dataset: AbstractDataset):
            return cls.C(path=cls.PROCESSED.path)

The ``transient=True`` flag tells the framework to delete intermediate data
once all downstream resources are COMPLETE.

Dataset ID Naming
-----------------

The dataset ID is derived from:

1. **Module path**: ``datamaestro_image.config.com.lecun`` → ``com.lecun``
2. **Class/function name**: ``MNIST`` → ``.mnist`` (lowercased)
3. **Final ID**: ``com.lecun.mnist``

The convention follows reversed domain names (like Java packages):

- ``com.lecun.mnist`` for http://yann.lecun.com/exdb/mnist/
- ``org.trec.robust04`` for https://trec.nist.gov/ ROBUST04 track
- ``io.huggingface.squad`` for HuggingFace datasets

The ``@dataset`` Annotation
============================

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
     - The base data type class (e.g., ``ImageClassification``). Can be inferred from the class hierarchy.
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
    class IgnoredName(MyType):
        ...

    # Replace last component only (in module com.example)
    @dataset(MyType, id=".v2")  # Results in com.example.v2
    class Original(MyType):
        ...

    # Empty string uses module path only
    @dataset(MyType, id="")  # Results in com.example (no class name)
    class Main(MyType):
        ...

Resources
=========

Resources are defined as class attributes on dataset classes. See the
:doc:`api/download` for the full resource API reference and all available
resource types.

Single Files
------------

Use :py:class:`~datamaestro.download.single.FileDownloader` for single file downloads:

.. code-block:: python

    from datamaestro.download.single import FileDownloader

    @dataset(url="http://example.com")
    class MyDataset(CSVData):
        DATA = FileDownloader("data.csv", "http://example.com/data.csv")

Archives
--------

Use :py:class:`~datamaestro.download.archive.ZipDownloader` or
:py:class:`~datamaestro.download.archive.TarDownloader` for archives:

.. code-block:: python

    from datamaestro.download.archive import ZipDownloader, TarDownloader

    @dataset(url="http://example.com")
    class ZippedDataset(MyData):
        DATA = ZipDownloader("data", "http://example.com/archive.zip")

    @dataset(url="http://example.com")
    class TarDataset(MyData):
        DATA = TarDownloader(
            "data", "http://example.com/archive.tar.gz",
            subpath="archive/subdir",
        )

HuggingFace Integration
-----------------------

.. code-block:: python

    from datamaestro.download.huggingface import HFDownloader

    @dataset(url="https://huggingface.co/datasets/squad")
    class Squad(QADataset):
        HF_DATA = HFDownloader("squad_data", "squad")

Links to Other Datasets
------------------------

Use :py:func:`~datamaestro.download.links.links` to reference other datasets:

.. code-block:: python

    from datamaestro.download.links import links

    @dataset(url="http://example.com")
    class ExtendedDataset(ExtendedData):
        BASE = links("base", "com.example.base_dataset")

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
    @dataset(url="http://example.com")
    class MNIST(ImageClassification):
        """Dataset description"""
        TRAIN_IMAGES = FileDownloader(...)
        ...

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
    class Robust04(IRDataset):
        ...

    @dataset(TRECBase, url="https://trec.nist.gov/...")
    class Robust05(IRDataset):
        ...

File Validation
===============

Use :py:class:`~datamaestro.utils.HashCheck` to validate downloaded files with checksums:

.. code-block:: python

    from datamaestro.download.single import FileDownloader
    from datamaestro.utils import HashCheck

    DATA = FileDownloader(
        "data.csv",
        "http://example.com/data.csv",
        checker=HashCheck("sha256", "abc123...")
    )

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


.. _deprecated-decorator-datasets:

Deprecated: Decorator-based Datasets
=====================================

.. deprecated::
   The decorator-based API still works but emits deprecation warnings.
   Migrate to the class-based approach described above.

The legacy approach uses function decorators to define datasets:

.. code-block:: python
    :caption: DEPRECATED — use class-based approach instead

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
        """The MNIST database"""
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

In this legacy pattern:

- Download decorators are stacked above ``@dataset``
- File paths are passed as arguments to the dataset function
- The function returns a dict or data object

See :ref:`deprecated-download-decorators` for the full list of deprecated
download decorator patterns.
