Download Resources
==================

Resources represent steps in a dataset preparation pipeline. They form a
directed acyclic graph (DAG) where each resource can depend on other resources.

Key concepts:

- **Two-path system**: resources write to ``transient_path`` during download,
  then the framework moves data to ``path`` and marks the resource as COMPLETE.
- **Three states**: NONE, PARTIAL, COMPLETE (persisted in ``.state.json``)
- **Transient resources**: intermediate resources that can be deleted after all
  dependents are COMPLETE (eager cleanup)

Resource Hierarchy
------------------

.. code-block:: text

    Resource (ABC)
    ├── FileResource      — produces a single file
    ├── FolderResource    — produces a directory
    ├── ValueResource     — produces an in-memory value (no files)
    ├── reference         — references another dataset
    └── Download          — (deprecated alias for Resource)

Resource Base Class
-------------------

.. autoclass:: datamaestro.download.Resource
   :members: name, dataset, transient, can_recover, dependencies, dependents,
             path, transient_path, state, download, prepare, cleanup,
             has_files, bind, apply

ResourceState
~~~~~~~~~~~~~

.. autoclass:: datamaestro.download.ResourceState
   :members:

FileResource
~~~~~~~~~~~~

.. autoclass:: datamaestro.download.FileResource
   :members: path, transient_path, prepare, stream, download

FolderResource
~~~~~~~~~~~~~~

.. autoclass:: datamaestro.download.FolderResource
   :members: path, transient_path, prepare, download

ValueResource
~~~~~~~~~~~~~

.. autoclass:: datamaestro.download.ValueResource
   :members: has_files, prepare


Defining Resources (Modern API)
===============================

Resources are defined as class attributes on dataset classes. The framework
automatically detects them and builds the dependency graph.

Single Files
------------

Package: ``datamaestro.download.single``

Use :py:class:`~datamaestro.download.single.FileDownloader` for single file downloads:

.. code-block:: python

    from datamaestro.download.single import FileDownloader
    from datamaestro.definitions import AbstractDataset, dataset

    @dataset(url="http://example.com")
    class MyDataset(CSVData):
        DATA = FileDownloader("data.csv", "http://example.com/data.csv")

        @classmethod
        def __create_dataset__(cls, dataset: AbstractDataset):
            return cls.C(path=cls.DATA.path)

.. autoclass:: datamaestro.download.single.FileDownloader

**Automatic Decompression:**

Files with ``.gz`` or ``.bz2`` extensions are automatically decompressed:

.. code-block:: python

    # Downloads and decompresses to data.txt
    DATA = FileDownloader(
        "data.txt", "http://example.com/data.txt.gz"
    )

ConcatDownloader
~~~~~~~~~~~~~~~~

Downloads multiple files and concatenates them:

.. autoclass:: datamaestro.download.single.ConcatDownloader

.. code-block:: python

    COMBINED = ConcatDownloader(
        "combined.txt",
        "http://example.com/part1.txt",
        "http://example.com/part2.txt",
        "http://example.com/part3.txt",
    )

Archives
--------

Package: ``datamaestro.download.archive``

Archive resources extract archives and produce a directory.

ZipDownloader
~~~~~~~~~~~~~

Downloads and extracts ZIP archives:

.. autoclass:: datamaestro.download.archive.ZipDownloader

.. code-block:: python

    from datamaestro.download.archive import ZipDownloader

    @dataset(url="http://example.com")
    class MyDataset(MyData):
        DATA = ZipDownloader("data", "http://example.com/archive.zip")

        @classmethod
        def __create_dataset__(cls, dataset: AbstractDataset):
            return cls.C(path=cls.DATA.path / "file.csv")

**Parameters:**

- ``varname``: Resource name
- ``url``: URL to the ZIP file
- ``files``: Optional list of files to extract (default: all)
- ``subpath``: Extract only a subdirectory

TarDownloader
~~~~~~~~~~~~~

Downloads and extracts TAR archives (including .tar.gz, .tar.bz2):

.. autoclass:: datamaestro.download.archive.TarDownloader

.. code-block:: python

    from datamaestro.download.archive import TarDownloader

    @dataset(url="http://example.com")
    class MyDataset(MyData):
        DATA = TarDownloader(
            "data", "http://example.com/archive.tar.gz"
        )

HuggingFace Integration
-----------------------

Package: ``datamaestro.download.huggingface``

For datasets hosted on HuggingFace Hub:

.. autoclass:: datamaestro.download.huggingface.HFDownloader

.. code-block:: python

    from datamaestro.download.huggingface import HFDownloader

    @dataset(url="https://huggingface.co/datasets/squad")
    class Squad(QADataset):
        HF_DATA = HFDownloader("squad_data", "squad")

Links
-----

Package: ``datamaestro.download.links``

Links reference other datasets or external data folders.

.. code-block:: python

    from datamaestro.download.links import links

    @dataset(url="http://example.com")
    class ExtendedDataset(ExtendedData):
        BASE = links("base", "com.example.base_dataset")

        @classmethod
        def __create_dataset__(cls, dataset: AbstractDataset):
            return cls.C(base=cls.BASE.prepare())

linkfolder
~~~~~~~~~~

Link to a configured data folder:

.. code-block:: python

    from datamaestro.download.links import linkfolder

    @dataset(url="http://example.com")
    class ExternalDataset(MyData):
        DATA = linkfolder("data", "mydata")

linkfile
~~~~~~~~

Link to a specific file in a data folder:

.. code-block:: python

    from datamaestro.download.links import linkfile

    @dataset(url="http://example.com")
    class SpecificFile(MyData):
        CSV = linkfile("csvfile", "mydata", "subdir/data.csv")

Internet Archive (Wayback Machine)
-----------------------------------

Package: ``datamaestro.download.wayback``

For datasets that are no longer available at their original URLs:

.. code-block:: python

    from datamaestro.download.wayback import wayback_documents

    @dataset(url="http://example.com")
    class ArchivedDataset(MyData):
        DATA = wayback_documents(
            "data", "http://defunct-website.com/data.csv",
            timestamp="20200101"
        )

Custom Downloads
----------------

Package: ``datamaestro.download.custom``

For complex download logic that doesn't fit standard patterns:

.. autoclass:: datamaestro.download.custom.Downloader
.. autofunction:: datamaestro.download.custom.custom_download

reference
---------

References another dataset instead of downloading:

.. autoclass:: datamaestro.download.reference

.. code-block:: python

    from datamaestro.download import reference

    @dataset(url="http://example.com")
    class DerivedDataset(MyData):
        BASE = reference("base", reference=other_dataset)


Transient Resources & Pipelines
================================

Resources can be marked as ``transient``, meaning their data can be deleted
after all downstream dependents reach COMPLETE state. This is useful for
intermediate files in processing pipelines.

.. code-block:: python

    from datamaestro.download.single import FileDownloader

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


Creating Custom Resource Handlers
==================================

Extend the download system by subclassing ``FileResource``, ``FolderResource``,
or ``ValueResource``:

.. code-block:: python

    from datamaestro.download import FileResource

    class MyProcessor(FileResource):
        """Process a source file into a numpy array."""

        @property
        def can_recover(self) -> bool:
            return False

        def __init__(self, filename, source, **kw):
            super().__init__(filename, **kw)
            self._dependencies = [source]

        def _download(self, destination):
            source_path = self.dependencies[0].path
            data = load(source_path)
            save(process(data), destination)

        @classmethod
        def from_source(cls, source):
            return cls("processed.npy", source)

    # Factory alias
    my_processor = MyProcessor.from_source

The ``_download(destination)`` method receives ``self.transient_path`` as
``destination``. After it returns, the framework moves data from
``transient_path`` to ``path`` and marks the resource as COMPLETE.


File Validation
===============

Package: ``datamaestro.utils``

Validate downloaded files with checksums:

.. autoclass:: datamaestro.utils.FileChecker
.. autoclass:: datamaestro.utils.HashCheck
   :members: __init__

.. code-block:: python

    from datamaestro.download.single import FileDownloader
    from datamaestro.utils import HashCheck

    DATA = FileDownloader(
        "data.csv",
        "http://example.com/data.csv",
        checker=HashCheck("sha256", "abc123def456...")
    )

**Supported hash algorithms:** ``md5``, ``sha1``, ``sha256``, ``sha512``


Two-Path Download Flow
======================

The framework orchestrates the download process for each resource:

1. **COMPLETE and not force** — skip (no-op)
2. **PARTIAL and not can_recover** — delete ``transient_path``, set NONE
3. **PARTIAL and can_recover** — leave ``transient_path`` in place for resumption
4. Call ``resource.download(force)`` — resource writes to ``transient_path``
5. **On success** — move ``transient_path`` → ``path``, set COMPLETE
6. **On failure** — if ``can_recover``, set PARTIAL; otherwise delete and set NONE
7. **Eager cleanup** — for each transient dependency where all dependents are COMPLETE, call ``cleanup()``


State Metadata File
===================

Resource states are persisted in ``<dataset.datapath>/.downloads/.state.json``:

.. code-block:: json

    {
      "version": 1,
      "resources": {
        "TRAIN_IMAGES": {"state": "complete"},
        "TRAIN_LABELS": {"state": "partial"}
      }
    }


.. _deprecated-download-decorators:

Deprecated: Download Decorators
===============================

.. deprecated::
   The decorator-based API still works but emits deprecation warnings.
   Migrate to the class-attribute approach described above.

Download decorators are applied above the ``@dataset`` decorator and pass
downloaded file paths as arguments to the dataset function.

.. code-block:: python

    from datamaestro.download.single import filedownloader
    from datamaestro.definitions import dataset

    # DEPRECATED — use class-attribute approach instead
    @filedownloader("data", "http://example.com/data.csv")
    @dataset(MyData)
    def my_dataset(data):  # 'data' receives the downloaded Path
        return MyData(path=data)

filedownloader (decorator)
--------------------------

.. code-block:: python

    # DEPRECATED
    @filedownloader("data.csv", "http://example.com/data.csv")
    @dataset(MyData)
    def compressed_dataset(data):
        return MyData(path=data)

concatdownload (decorator)
--------------------------

.. code-block:: python

    # DEPRECATED
    @concatdownload(
        "combined",
        "http://example.com/part1.txt",
        "http://example.com/part2.txt",
    )
    @dataset(MyData)
    def concatenated_dataset(combined):
        return MyData(path=combined)

zipdownloader / tardownloader (decorator)
-----------------------------------------

.. code-block:: python

    from datamaestro.download.archive import zipdownloader, tardownloader

    # DEPRECATED
    @zipdownloader("data", "http://example.com/archive.zip")
    @dataset(MyData)
    def zipped_dataset(data):
        return MyData(path=data / "file.csv")

    # DEPRECATED
    @tardownloader("data", "http://example.com/archive.tar.gz")
    @dataset(MyData)
    def tar_dataset(data):
        return MyData(path=data / "file.csv")

Multiple decorators (deprecated)
---------------------------------

.. code-block:: python

    # DEPRECATED
    @filedownloader("train", "http://example.com/train.csv")
    @filedownloader("test", "http://example.com/test.csv")
    @dataset(MyData)
    def multi_resource_dataset(train, test):
        return MyData(train_path=train, test_path=test)

Custom handler (deprecated)
----------------------------

.. code-block:: python

    from datamaestro.download import Download

    # DEPRECATED — use FileResource / FolderResource instead
    class MyDownload(Download):
        def __init__(self, varname, custom_param):
            super().__init__(varname)
            self.custom_param = custom_param

        def prepare(self):
            return self._download_and_process()

        def download(self, force=False):
            if force or not self._is_cached():
                self._do_download()

        def hasfiles(self) -> bool:
            return True

Deprecated Names
----------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Deprecated
     - Replacement
   * - ``Download`` (base class)
     - ``Resource``
   * - ``hasfiles()``
     - ``has_files()``
   * - ``Resource.definition``
     - ``Resource.dataset``
   * - ``Resource.varname``
     - ``Resource.name``
   * - ``@filedownloader(...)`` (decorator)
     - ``FileDownloader(...)`` (class attribute)
   * - ``SingleDownload``
     - ``FileDownloader``
