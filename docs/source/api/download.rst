Download Decorators
===================

Download decorators specify how to fetch resources for datasets. They are applied
as decorators above the ``@dataset`` decorator and pass downloaded file paths
as arguments to the dataset function.

Overview
--------

.. code-block:: python

    from datamaestro.download.single import filedownloader
    from datamaestro.definitions import dataset

    @filedownloader("data", "http://example.com/data.csv")
    @dataset(MyData)
    def my_dataset(data):  # 'data' receives the downloaded Path
        return MyData(path=data)

Single Files
============

Package: ``datamaestro.download.single``

filedownloader
--------------

Downloads a single file, optionally decompressing it.

.. autofunction:: datamaestro.download.single.filedownloader

**Parameters:**

- ``varname``: Name of the argument passed to the dataset function
- ``url``: URL to download
- ``checker``: Optional file validation (see File Validation below)

**Automatic Decompression:**

Files with ``.gz`` or ``.bz2`` extensions are automatically decompressed:

.. code-block:: python

    # Downloads and decompresses to data.txt
    @filedownloader("data", "http://example.com/data.txt.gz")
    @dataset(MyData)
    def compressed_dataset(data):
        return MyData(path=data)

concatdownload
--------------

Downloads multiple files and concatenates them:

.. autofunction:: datamaestro.download.single.concatdownload

.. code-block:: python

    @concatdownload(
        "combined",
        "http://example.com/part1.txt",
        "http://example.com/part2.txt",
        "http://example.com/part3.txt",
    )
    @dataset(MyData)
    def concatenated_dataset(combined):
        return MyData(path=combined)

Archives
========

Package: ``datamaestro.download.archive``

Archive downloaders extract archives and pass the extraction directory path.

zipdownloader
-------------

Downloads and extracts ZIP archives:

.. autofunction:: datamaestro.download.archive.zipdownloader

**Parameters:**

- ``varname``: Argument name
- ``url``: URL to the ZIP file
- ``files``: Optional list of files to extract (default: all)
- ``subpath``: Extract only a subdirectory

.. code-block:: python

    from datamaestro.download.archive import zipdownloader

    # Extract entire archive
    @zipdownloader("data", "http://example.com/archive.zip")
    @dataset(MyData)
    def full_archive(data):
        return MyData(path=data / "file.csv")

    # Extract only specific files
    @zipdownloader("data", "http://example.com/archive.zip",
                   files=["train.csv", "test.csv"])
    @dataset(MyData)
    def partial_archive(data):
        return MyData(path=data / "train.csv")

    # Extract a subdirectory
    @zipdownloader("data", "http://example.com/archive.zip",
                   subpath="archive/data")
    @dataset(MyData)
    def subdir_archive(data):
        return MyData(path=data / "file.csv")

tardownloader
-------------

Downloads and extracts TAR archives (including .tar.gz, .tar.bz2):

.. autofunction:: datamaestro.download.archive.tardownloader

.. code-block:: python

    from datamaestro.download.archive import tardownloader

    @tardownloader("data", "http://example.com/archive.tar.gz")
    @dataset(MyData)
    def tar_dataset(data):
        return MyData(path=data / "file.csv")

Links
=====

Package: ``datamaestro.download.links``

Links reference other datasets or external data folders.

links
-----

Link to another dataset:

.. autofunction:: datamaestro.download.links.links

.. code-block:: python

    from datamaestro.download.links import links

    @links("base", "com.example.base_dataset")
    @dataset(ExtendedData)
    def extended_dataset(base):
        # 'base' is the prepared base dataset
        return ExtendedData(
            base_data=base.train,
            extra_info="additional processing"
        )

linkfolder
----------

Link to a configured data folder:

.. autofunction:: datamaestro.download.links.linkfolder

.. code-block:: python

    from datamaestro.download.links import linkfolder

    # Requires: datamaestro datafolders set mydata /path/to/data
    @linkfolder("data", "mydata")
    @dataset(MyData)
    def external_dataset(data):
        return MyData(path=data / "file.csv")

linkfile
--------

Link to a specific file in a data folder:

.. autofunction:: datamaestro.download.links.linkfile

.. code-block:: python

    from datamaestro.download.links import linkfile

    @linkfile("csvfile", "mydata", "subdir/data.csv")
    @dataset(MyData)
    def specific_file(csvfile):
        return MyData(path=csvfile)

Internet Archive (Wayback Machine)
==================================

Package: ``datamaestro.download.wayback``

For datasets that are no longer available at their original URLs:

.. autofunction:: datamaestro.download.wayback.wayback_documents

.. code-block:: python

    from datamaestro.download.wayback import wayback_documents

    @wayback_documents("data", "http://defunct-website.com/data.csv",
                       timestamp="20200101")
    @dataset(MyData)
    def archived_dataset(data):
        return MyData(path=data)

Google Drive Sync
=================

Package: ``datamaestro.download.sync``

For datasets hosted on Google Drive:

.. autofunction:: datamaestro.download.sync.gsync

.. code-block:: python

    from datamaestro.download.sync import gsync

    @gsync("data", "1ABC123xyz")  # Google Drive file ID
    @dataset(MyData)
    def gdrive_dataset(data):
        return MyData(path=data)

Custom Downloads
================

Package: ``datamaestro.download.custom``

For complex download logic that doesn't fit standard patterns:

.. autoclass:: datamaestro.download.custom.Downloader
.. autofunction:: datamaestro.download.custom.custom_download

.. code-block:: python

    from datamaestro.download.custom import Downloader, custom_download

    class MyCustomDownloader(Downloader):
        def download(self, destination):
            # Custom download logic
            # Write files to destination directory
            pass

    @custom_download("data", MyCustomDownloader())
    @dataset(MyData)
    def custom_dataset(data):
        return MyData(path=data)

File Validation
===============

Package: ``datamaestro.utils``

Validate downloaded files with checksums:

.. autoclass:: datamaestro.utils.FileChecker
.. autoclass:: datamaestro.utils.HashCheck
   :members: __init__

.. code-block:: python

    from datamaestro.download.single import filedownloader
    from datamaestro.utils import HashCheck

    @filedownloader(
        "data",
        "http://example.com/data.csv",
        checker=HashCheck("sha256", "abc123def456...")
    )
    @dataset(MyData)
    def validated_dataset(data):
        return MyData(path=data)

**Supported hash algorithms:**

- ``md5``
- ``sha1``
- ``sha256``
- ``sha512``

Multiple Resources
==================

Combine multiple download decorators:

.. code-block:: python

    @filedownloader("train", "http://example.com/train.csv")
    @filedownloader("test", "http://example.com/test.csv")
    @zipdownloader("extra", "http://example.com/extra.zip")
    @dataset(MyData)
    def multi_resource_dataset(train, test, extra):
        return MyData(
            train_path=train,
            test_path=test,
            extra_path=extra / "data.csv"
        )

Order matters: decorators are applied bottom-up, so resources are downloaded
in the order they appear (top to bottom).

Creating Custom Download Handlers
=================================

Extend the download system with custom handlers:

.. code-block:: python

    from datamaestro.download import Download

    class MyDownload(Download):
        def __init__(self, varname: str, custom_param: str):
            super().__init__(varname)
            self.custom_param = custom_param

        def prepare(self):
            """Return the data/path for the dataset function"""
            # Called when building the dataset
            return self._download_and_process()

        def download(self, force=False):
            """Perform the actual download"""
            # Called when downloading resources
            if force or not self._is_cached():
                self._do_download()

        def hasfiles(self) -> bool:
            """Return True if this handler creates files"""
            return True

        def _download_and_process(self):
            # Implementation
            ...

    def mydownloader(varname: str, custom_param: str):
        """Decorator for my custom download handler"""
        def decorator(dataset):
            download = MyDownload(varname, custom_param)
            download.register(dataset)
            return dataset
        return decorator
