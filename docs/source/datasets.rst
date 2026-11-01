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

Dataset Utilities
-----------------

``Dataset.data_path``
    A class property that returns the ``Path`` where this dataset's data is
    stored on disk. This is a shortcut for ``MyDataset.__dataset__.datapath``:

    .. code-block:: python

        @dataset(url="http://example.com")
        class Documents(Dataset):
            ...

        @dataset(url="http://example.com")
        class MyTask(Dataset):
            DOCS = reference(Documents)

            def config(self) -> TaskData:
                # Use Documents.data_path to locate sibling data
                store_path = Documents.data_path / "docstore"
                return TaskData.C(path=store_path)

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
2. **Class/function name**: CamelCase is converted to dot-separated lowercase
   (e.g., ``MNIST`` → ``mnist``, ``ProcessedMNIST`` → ``processed.mnist``,
   ``TrainTexttripleFull`` → ``train.texttriple.full``)
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
     - Override the automatic ID. Use a ``"."`` prefix to append to the module path (e.g., ``".8.topics"`` becomes ``module.path.8.topics``).
   * - ``url``
     - URL to the dataset's homepage.
   * - ``doi``
     - DOI of the associated paper.
   * - ``timestamp``
     - Version timestamp for evolving datasets.
   * - ``size``
     - Dataset size (for documentation).
   * - ``requires``
     - Python modules needed by the dataset that are not dependencies of the
       repository (see :ref:`dataset-requirements`).
   * - ``as_prepare`` (deprecated)
     - If True, the function receives the dataset object for manual resource handling.

ID Override Examples
--------------------

.. code-block:: python

    # Full ID override
    @dataset(MyType, id="org.example.custom")
    class IgnoredName(MyType):
        ...

    # Append suffix to module path (in module gov.nist.trec.adhoc)
    @dataset(MyType, id=".8.topics")  # Results in gov.nist.trec.adhoc.8.topics
    class Trec8Topics(MyType):
        ...

    # Single component suffix (in module com.example)
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

.. _dataset-requirements:

Python Requirements
-------------------

Some datasets need Python packages that the repository does not depend on
(e.g. HuggingFace ``datasets``, or a parser for an exotic format), typically
because they are heavy and only useful for a few datasets. They are declared
with ``requires``, so that datamaestro checks them *before* downloading
anything and tells users what to install, instead of failing in the middle of
a download with an ``ImportError``:

.. code-block:: python

    @dataset(
        url="https://huggingface.co/datasets/org/name",
        requires={"datasets": "datamaestro_myplugin[huggingface]"},
    )
    class MyDataset(Dataset):
        def config(self) -> MyHFData:
            ...

``requires`` accepts a module name (``"datasets"``), a list of module names,
or a mapping from module name to what should be installed to get it, as a
Python requirement — typically an extra of the plugin, so that versions are
handled in one place (the plugin's ``pyproject.toml``):

.. code-block:: toml

    [project.optional-dependencies]
    huggingface = ["datasets>=2.19"]

Requirements are checked when the dataset is downloaded (``prepare_dataset``,
``datamaestro download/prepare``, or experimaestro preparing a task). If some
are missing, the download fails with a
:class:`~datamaestro.requirements.MissingRequirementError` (an
``ImportError``) listing them::

    Missing Python requirements for dataset org.example.name:
      - datasets: not installed
    Please install: datamaestro_myplugin[huggingface]

A directory named like a required module (without ``__init__.py``) in the
current directory shadows the installed package; it is reported as such
rather than failing later with a confusing error.

Requirements are shown by ``datamaestro info`` (with ``[missing]`` for those
not available) and in the generated documentation.

**Data types.** When a requirement comes from the data type rather than from
the dataset, declare it on the type with
:func:`~datamaestro.requirements.requires`: every dataset whose configuration
is of this type (or of a subclass) inherits it. This is how
:class:`~datamaestro.data.huggingface.HuggingFaceDataset` declares its
dependency on ``datasets``. A dataset can still give a more precise
installation target for the same module (as in the example above), which
takes precedence:

.. code-block:: python

    from datamaestro.requirements import require, requires

    @requires("pyarrow")
    class MyParquetData(Base):
        def iter(self):
            # Checked here too: the configuration might not come from a
            # registered dataset
            require(self.__class__, what="MyParquetData")
            import pyarrow.parquet as pq
            ...

Avoid importing optional modules at the top level of a dataset definition
module: import them where they are used, so that the dataset can be listed,
documented and checked even when they are missing.

Referencing Other Datasets
--------------------------

Use :py:class:`~datamaestro.download.reference` to declare a dependency on
another dataset class. The referenced dataset is prepared automatically when
needed:

.. code-block:: python

    from datamaestro.download import reference

    @dataset(url="http://example.com")
    class MyTask(TaskData):
        DOCUMENTS = reference(DocumentsDataset)

        def config(self) -> TaskData:
            return TaskData.C(
                documents=self.DOCUMENTS.config(),
            )

Call ``.config()`` on the reference resource to obtain the referenced dataset's
configuration object (this is equivalent to ``.prepare()``).

.. note::

    ``reference()`` accepts the dataset class as its first positional argument.
    The older keyword form ``reference(reference=DocumentsDataset)`` still works
    but is no longer necessary.

Links to Other Datasets (by ID)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use :py:func:`~datamaestro.download.links.links` to reference datasets by their
string ID rather than by class:

.. code-block:: python

    from datamaestro.download.links import links

    @dataset(url="http://example.com")
    class ExtendedDataset(ExtendedData):
        BASE = links("base", "com.example.base_dataset")

Dataset Families
----------------

When a module registers many similar datasets (one per language, per
sub-collection, per split, …), writing one class per dataset is not
practical. :py:class:`~datamaestro.factory.DatasetFactory` creates the
dataset classes from a loop: it builds the
:py:class:`~datamaestro.definitions.Dataset` subclass, applies the
``@dataset`` decorator, and adds the class to the module namespace so that
datamaestro discovers it.

Example: a multilingual collection with, for each language, a corpus, and
for each split, the topics and the task combining the corpus and the
topics:

.. code-block:: python

    # src/datamaestro_myplugin/config/org/example/multilingual.py
    from experimaestro import Param

    from datamaestro.data import Base, File
    from datamaestro.download import reference
    from datamaestro.download.single import FileDownloader
    from datamaestro.factory import DatasetFactory

    URL = "https://example.org/multilingual"

    # language -> available splits
    LANGUAGES = {"en": ["train", "dev"], "sw": ["dev"]}


    class Task(Base):
        """A corpus with a set of topics"""

        corpus: Param[File]
        topics: Param[File]


    def register():
        datasets = DatasetFactory(
            __name__, prefix="org.example.multilingual", class_prefix="Multilingual"
        )

        for lang, splits in LANGUAGES.items():
            # org.example.multilingual.<lang>.corpus
            # The keyword gives the resource name (here, CORPUS)
            corpus = datasets.file(
                f"{lang}.corpus",
                File,
                url=URL,
                CORPUS=FileDownloader("corpus.jsonl", f"{URL}/{lang}/corpus.jsonl"),
            )

            for split in splits:
                # org.example.multilingual.<lang>.<split>.topics
                topics = datasets.file(
                    f"{lang}.{split}.topics",
                    File,
                    TOPICS=FileDownloader("topics.tsv", f"{URL}/{lang}/{split}.tsv"),
                )

                # org.example.multilingual.<lang>.<split>: any configuration,
                # built by a config(self) function from the resources
                def config(self) -> Task:
                    return Task.C(
                        corpus=self.CORPUS.config(), topics=self.TOPICS.config()
                    )

                datasets.register(
                    f"{lang}.{split}",
                    Task,
                    config,
                    resources={
                        "CORPUS": reference(corpus),
                        "TOPICS": reference(topics),
                    },
                    url=URL,
                    doc=f"Multilingual task ({lang}, {split})",
                )


    register()

This registers ``org.example.multilingual.en.corpus``,
``org.example.multilingual.en.train.topics``,
``org.example.multilingual.en.train``, and so on; e.g.
``prepare_dataset("org.example.multilingual.sw.dev")`` downloads the Swahili
corpus and dev topics, and returns a ``Task``.

A few points to keep in mind:

- Dataset ids are always explicit (``<prefix>.<id>``); class names are
  derived from the ids (here, ``Multilingual_en_train``) and only matter
  for discovery. A name already in use gets a numeric suffix (``a-b`` and
  ``a_b`` give ``a_b`` and ``a_b_2``), or can be set with ``name=``.
- Resource names (``CORPUS``, ``TOPICS``) are used to track the download
  state, so keep them stable once datasets have been downloaded.
- Each registered dataset has its own resources and data folder. This
  differs from :doc:`dataset variants <api/variants>`, where a single
  dataset (and its resources) is parameterized at prepare time: use
  variants when the variants share the same source (e.g. different
  configurations of one HuggingFace dataset), and a factory when each
  dataset has its own files.

Plugins can subclass :py:class:`~datamaestro.factory.DatasetFactory` to add
domain-specific builders (e.g. a document store built from a corpus file)
on top of :py:meth:`~datamaestro.factory.DatasetFactory.register`.

.. autoclass:: datamaestro.factory.DatasetFactory
    :members: register, file, full_id, class_name

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
