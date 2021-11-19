import logging
from pathlib import Path
import zipfile
import shutil
import urllib3
import tarfile
import re
import hashlib
from typing import List, Set, Optional
from datamaestro.download import Download, initialized
from datamaestro.utils import CachedFile, HashCheck, FileChecker


class ArchiveDownloader(Download):
    """Abstract class for all archive related extractors"""

    def __init__(
        self,
        varname,
        url: str,
        subpath: str = None,
        checker: FileChecker = None,
        files: Set[str] = None,
    ):
        """Downloads and extract the content of the archive

        Args:
            varname: The name of the variable when defining the dataset
            url: The archive URL
            checker: the hash check for the downloaded file, composed of two
            subpath: A subpath in the archive; only files from this subpath will be extracted
            files: A set of files; if present, only extract those
        """
        super().__init__(varname)
        self.url = url
        self.subpath = subpath
        self.checker = checker
        self._files = set(files) if files else None
        if self.subpath and not self.subpath.endswith("/"):
            self.subpath = self.subpath + "/"

    def postinit(self):
        # Define the path
        p = urllib3.util.parse_url(self.url)
        name = self._name(Path(p.path).name)

        if len(self.definition.resources) > 1:
            self.path = self.definition.datapath / name
        else:
            self.path = self.definition.datapath

    @initialized
    def prepare(self):
        return self.path

    @property
    def extractall(self):
        """Returns whether everything can be extracted"""
        return self._files is None and self.subpath is None

    def filter(self, iterable, getname):
        L = len(self.subpath) if self.subpath else 0

        for info in iterable:
            name = getname(info)
            logging.debug("Looking at %s", name)
            if self._files and not (name in self._files):
                continue

            if self.subpath and name.startswith(self.subpath):
                yield info, name[L:]

            yield info, name

    @initialized
    def download(self, force=False):
        # Already downloaded
        destination = self.definition.datapath
        if destination.is_dir():
            return

        logging.info("Downloading %s into %s", self.url, destination)

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmpdestination = destination.with_suffix(".tmp")
        if tmpdestination.exists():
            logging.warn("Removing temporary directory %s", tmpdestination)
            shutil.rmtree(tmpdestination)

        with self.context.downloadURL(self.url) as file:
            if self.checker:
                self.checker.check(file.path)
            self.unarchive(file, tmpdestination)

        # Look at the content
        for ix, path in enumerate(tmpdestination.iterdir()):
            if ix > 1:
                break

        # Just one folder: move
        if ix == 0 and path.is_dir():
            logging.info(
                "Moving single file/directory {} into destination {}".format(
                    path, destination
                )
            )
            shutil.move(str(path), str(destination))
            shutil.rmtree(tmpdestination)
        else:
            shutil.move(tmpdestination, destination)


class zipdownloader(ArchiveDownloader):
    """ZIP Archive handler"""

    def _name(self, name):
        return re.sub(r"\.zip$", "", name)

    def unarchive(self, file, destination: Path):
        logging.info("Unzipping file")
        with zipfile.ZipFile(file.path) as zip:
            if self.extractall:
                zip.extractall(destination)
            else:
                for zip_info, name in self.filter(
                    zip.infolist(), lambda zip_info: zip_info.filename
                ):
                    if zip_info.is_dir():
                        (destination / name).mkdir()
                    else:
                        logging.info(
                            "File %s (%s) to %s",
                            zip_info.filename,
                            name,
                            destination / name,
                        )
                        with zip.open(zip_info) as fp, (destination / name).open(
                            "wb"
                        ) as out:
                            shutil.copyfileobj(fp, out)


class tardownloader(ArchiveDownloader):
    """TAR archive handler"""

    def _name(self, name):
        return re.sub(r"\.tar(\.gz|\.bz\|xz)?$", "", name)

    def unarchive(self, file: CachedFile, destination: Path):
        logging.info("Unarchiving file")
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
                        logging.info(
                            "File %s (%s) to %s", info.name, name, destination / name,
                        )
                        logging.info("Extracting into %s", destination / name)
                        tar.extract(info, destination / name)
