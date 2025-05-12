Download decorators
-------------------

Single files
============

Package `datamaestro.download.single`

.. autofunction:: datamaestro.download.single.filedownloader

.. autofunction:: datamaestro.download.single.concatdownload





Archives
========

Package `datamaestro.download.archive`

The different archive download methods associated with the archive
a Path. They allow to filter the archives with the ``files`` argument
and ``subpath`` (to only include a sub-folder of the archive)

.. autofunction:: datamaestro.download.archive.zipdownloader

.. autofunction:: datamaestro.download.archive.tardownloader


Links
=====

Package `datamaestro.download.links`

.. autofunction:: datamaestro.download.links.links

.. autofunction:: datamaestro.download.links.linkfolder

.. autofunction:: datamaestro.download.links.linkfile


Other
=====

.. autofunction:: datamaestro.download.wayback.wayback_documents



Syncing
=======

Package `datamaestro.download.sync`

.. autofunction:: datamaestro.download.sync.gsync

Utility functions
=================

File hashes can be checked with the following checker

.. autoclass:: datamaestro.utils.FileChecker
.. autoclass:: datamaestro.utils.HashCheck
       :members: __init__


Custom
======

.. autoclass:: datamaestro.download.custom.Downloader
.. autofunction:: datamaestro.download.custom.custom_download
