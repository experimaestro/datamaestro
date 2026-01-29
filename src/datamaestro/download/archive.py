"""Archive download resources.

Provides FolderResource subclasses for downloading and extracting
ZIP and TAR archives.
"""

from __future__ import annotations

import logging
import re
import shutil
import tarfile
import zipfile
from pathlib import Path
from typing import Set

import urllib3

from datamaestro.download import FolderResource
from datamaestro.utils import CachedFile, FileChecker

logger = logging.getLogger(__name__)


class ArchiveDownloader(FolderResource):
    """Abstract base for all archive-related extractors.

    Usage as class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = ZipDownloader.apply(
                "archive", "http://example.com/data.zip"
            )

    Usage as decorator (deprecated)::

        @zipdownloader("archive", "http://example.com/data.zip")
        @dataset(Base)
        def my_dataset(archive): ...
    """

    def __init__(
        self,
        varname: str,
        url: str,
        subpath: str | None = None,
        checker: FileChecker | None = None,
        files: Set[str] | None = None,
        *,
        transient: bool = False,
    ):
        """Downloads and extract the content of the archive.

        Args:
            varname: The name of the variable when defining the dataset.
            url: The archive URL.
            subpath: A subpath in the archive; only files from this
                subpath will be extracted.
            checker: The hash check for the downloaded file.
            files: A set of files; if present, only extract those.
            transient: If True, data can be deleted after dependents
                complete.
        """
        super().__init__(varname=varname, transient=transient)
        self.url = url
        self.subpath = subpath
        self.checker = checker
        self._files = set(files) if files else None
        if self.subpath and not self.subpath.endswith("/"):
            self.subpath = self.subpath + "/"

    def postinit(self):
        # Define the path
        p = urllib3.util.parse_url(self.url)
        self._archive_name = self._name(Path(p.path).name)

    @property
    def path(self) -> Path:
        """Final path to the extracted directory."""
        if not self._post:
            self._post = True
            self.postinit()

        if len(self.dataset.resources) > 1:
            return self.dataset.datapath / self._archive_name
        return self.dataset.datapath

    @property
    def transient_path(self) -> Path:
        """Temporary path for extraction."""
        if not self._post:
            self._post = True
            self.postinit()

        if len(self.dataset.resources) > 1:
            return self.dataset.datapath / ".downloads" / self._archive_name
        return self.dataset.datapath / ".downloads" / self.name

    @property
    def extractall(self):
        """Returns whether everything can be extracted."""
        return self._files is None and self.subpath is None

    def filter(self, iterable, getname):
        L = len(self.subpath) if self.subpath else 0

        for info in iterable:
            name = getname(info)
            logger.debug("Looking at %s", name)
            if self._files and name not in self._files:
                continue

            if self.subpath and name.startswith(self.subpath):
                yield info, name[L:]

            if not self.subpath:
                yield info, name

    def _download(self, destination: Path) -> None:
        logger.info("Downloading %s into %s", self.url, destination)

        destination.parent.mkdir(parents=True, exist_ok=True)

        with self.context.downloadURL(self.url) as file:
            if self.checker:
                self.checker.check(file.path)
            self.unarchive(file, destination)

        # Look at the content - if single directory, unwrap
        children = list(destination.iterdir())
        if len(children) == 1 and children[0].is_dir():
            single_dir = children[0]
            logger.info(
                "Moving single directory %s into destination %s",
                single_dir,
                destination,
            )
            # Move contents up one level
            tmp = destination.with_suffix(".unwrap")
            shutil.move(str(single_dir), str(tmp))
            shutil.rmtree(destination)
            shutil.move(str(tmp), str(destination))

    def unarchive(self, file, destination: Path):
        raise NotImplementedError()

    def _name(self, name: str) -> str:
        raise NotImplementedError()


class ZipDownloader(ArchiveDownloader):
    """ZIP Archive handler."""

    def _name(self, name):
        return re.sub(r"\.zip$", "", name)

    def unarchive(self, file, destination: Path):
        logger.info("Unzipping file")
        with zipfile.ZipFile(file.path) as zip:
            if self.extractall:
                zip.extractall(destination)
            else:
                for zip_info, name in self.filter(
                    zip.infolist(),
                    lambda zip_info: zip_info.filename,
                ):
                    if zip_info.is_dir():
                        (destination / name).mkdir()
                    else:
                        logger.info(
                            "File %s (%s) to %s",
                            zip_info.filename,
                            name,
                            destination / name,
                        )
                        with (
                            zip.open(zip_info) as fp,
                            (destination / name).open("wb") as out,
                        ):
                            shutil.copyfileobj(fp, out)


class TarDownloader(ArchiveDownloader):
    """TAR archive handler."""

    def _name(self, name):
        return re.sub(r"\.tar(\.gz|\.bz\|xz)?$", "", name)

    def unarchive(self, file: CachedFile, destination: Path):
        logger.info("Unarchiving file")
        if self.subpath:
            raise NotImplementedError()

        with tarfile.TarFile.open(file.path) as tar:
            if self.extractall:
                tar.extractall(destination)
            else:
                for info, name in self.filter(tar, lambda info: info.name):
                    if info.isdir():
                        (destination / name).mkdir()
                    else:
                        logger.info(
                            "File %s (%s) to %s",
                            info.name,
                            name,
                            destination / name,
                        )
                        logger.info(
                            "Extracting into %s",
                            destination / name,
                        )
                        tar.extract(info, destination / name)


# Factory aliases for backward compat and convenient usage
zipdownloader = ZipDownloader.apply
tardownloader = TarDownloader.apply
