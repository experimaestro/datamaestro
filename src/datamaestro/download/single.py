"""Single file download resources.

Provides FileResource subclasses for downloading individual files
from URLs, with optional transforms and integrity checking.
"""

from __future__ import annotations

import io
import gzip
import logging
import os
import os.path as op
import shutil
import tarfile
from pathlib import Path

import urllib3

from datamaestro.download import FileResource
from datamaestro.stream import Transform
from datamaestro.utils import copyfileobjs

logger = logging.getLogger(__name__)


def open_ext(*args, **kwargs):
    """Opens a file according to its extension."""
    name = args[0]
    if name.endswith(".gz"):
        return gzip.open(*args, *kwargs)
    return io.open(*args, **kwargs)


class FileDownloader(FileResource):
    """Downloads a single file from a URL.

    Supports optional transforms (e.g., gzip decompression)
    and integrity checking.

    Usage as class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = FileDownloader.apply(
                "data.csv", "http://example.com/data.csv.gz"
            )

    Usage as decorator (deprecated)::

        @filedownloader("data.csv", "http://example.com/data.csv.gz")
        @dataset(Base)
        def my_dataset(data): ...
    """

    def __init__(
        self,
        filename: str,
        url: str,
        size: int | None = None,
        transforms: Transform | None = None,
        checker=None,
        *,
        varname: str | None = None,
        transient: bool = False,
    ):
        """
        Args:
            filename: The filename within the data folder; the variable
                name corresponds to the filename without the extension.
            url: The URL to download.
            size: Expected size in bytes (or None).
            transforms: Transform the file before storing it.
                Auto-detected from URL path if None.
            checker: File integrity checker.
            varname: Explicit resource name.
            transient: If True, data can be deleted after dependents
                complete.
        """
        super().__init__(filename, varname=varname, transient=transient)
        self.url = url
        self.checker = checker
        self.size = size

        p = urllib3.util.parse_url(self.url)
        path = Path(Path(p.path).name)
        self.transforms = transforms if transforms else Transform.createFromPath(path)

    def _download(self, destination: Path) -> None:
        logger.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download (cache)
        with self.context.downloadURL(self.url, size=self.size) as file:
            # Transform if need be
            if self.transforms:
                logger.info("Transforming file")
                with (
                    self.transforms(file.path.open("rb")) as stream,
                    destination.open("wb") as out,
                ):
                    if self.checker:
                        copyfileobjs(stream, [out, self.checker])
                        self.checker.close()
                    else:
                        shutil.copyfileobj(stream, out)
            else:
                logger.info("Keeping original downloaded file %s", file.path)
                if self.checker:
                    self.checker.check(file.path)
                (shutil.copy if file.keep else shutil.move)(file.path, destination)

        logger.info("Created file %s", destination)


# Factory alias for backward compat and convenient usage
filedownloader = FileDownloader.apply


class ConcatDownloader(FileResource):
    """Concatenate all files from an archive into a single file.

    Usage as class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = ConcatDownloader.apply(
                "data.txt", "http://example.com/data.tar.gz"
            )
    """

    def __init__(
        self,
        filename: str,
        url: str,
        transforms=None,
        *,
        varname: str | None = None,
        transient: bool = False,
    ):
        """
        Args:
            filename: The filename within the data folder; the variable
                name corresponds to the filename without the extension.
            url: The URL to download.
            transforms: Transform the file before storing it.
            varname: Explicit resource name.
            transient: If True, data can be deleted after dependents
                complete.
        """
        super().__init__(filename, varname=varname, transient=transient)
        self.url = url
        self.transforms = transforms

    def _download(self, destination: Path) -> None:
        with (
            self.context.downloadURL(self.url) as dl,
            tarfile.open(dl.path) as archive,
        ):
            destination.parent.mkdir(parents=True, exist_ok=True)

            with open(destination, "wb") as out:
                for tarinfo in archive:
                    if tarinfo.isreg():
                        transforms = self.transforms or Transform.createFromPath(
                            Path(tarinfo.name)
                        )
                        logger.debug("Processing file %s", tarinfo.name)
                        with transforms(archive.fileobject(archive, tarinfo)) as fp:
                            shutil.copyfileobj(fp, out)


# Factory alias for backward compat
concatdownload = ConcatDownloader.apply


# --- Backward compat aliases ---
# Keep old class names importable but they now point to new classes

SingleDownload = FileDownloader
