import logging
import shutil
import tarfile
import io
import tempfile
import gzip
import os.path as op, os
import urllib3
from pathlib import Path
from tempfile import NamedTemporaryFile
import re
from docstring_parser import parse
from datamaestro.utils import copyfileobjs
from datamaestro.stream import Transform
from datamaestro.download import Download


def open_ext(*args, **kwargs):
    """Opens a file according to its extension"""
    name = args[0]
    if name.endswith(".gz"):
        return gzip.open(*args, *kwargs)
    return io.open(*args, **kwargs)


class SingleDownload(Download):
    def __init__(self, filename: str):
        super().__init__(re.sub(r"\..*$", "", filename))
        self.name = filename

    @property
    def path(self):
        return self.definition.datapath / self.name

    def prepare(self):
        return self.path

    def download(self, force=False):
        if not self.path.is_file():
            self._download(self.path)


class filedownloader(SingleDownload):
    def __init__(
        self, filename: str, url: str, size: int = None, transforms=None, checker=None
    ):
        """Downloads a file given by a URL

        Args:
            filename: The filename within the data folder; the variable name corresponds to the filename without the extension.
            url: The URL to download
            transforms: Transform the file before storing it
            size: size in bytes (or None)
        """
        super().__init__(filename)
        self.url = url
        self.checker = checker
        self.size = size

        p = urllib3.util.parse_url(self.url)
        path = Path(Path(p.path).name)
        self.transforms = transforms if transforms else Transform.createFromPath(path)

    def _download(self, destination):
        logging.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download (cache)
        with self.context.downloadURL(self.url, size=self.size) as file:
            # Transform if need be
            if self.transforms:
                logging.info("Transforming file")
                with self.transforms(file.path.open("rb")) as stream, destination.open(
                    "wb"
                ) as out:
                    if self.checker:
                        copyfileobjs(stream, [out, self.checker])
                        self.checker.close()
                    else:
                        shutil.copyfileobj(stream, out)
            else:
                logging.info("Keeping original downloaded file %s", file.path)
                if self.checker:
                    self.checker.check(file.path)
                (shutil.copy if file.keep else shutil.move)(file.path, destination)

        logging.info("Created file %s" % destination)


class concatdownload(SingleDownload):
    """Concatenate all files in an archive"""

    def __init__(self, filename: str, url: str, transforms=None):
        """Concat the files in an archive

        Args:
            filename: The filename within the data folder; the variable name corresponds to the filename without the extension
            url: The URL to download
            transforms: Transform the file before storing it
        """
        super().__init__(filename)
        self.url = url
        self.transforms = transforms

    def _download(self, destination):
        with self.context.downloadURL(self.url) as dl, tarfile.open(dl.path) as archive:
            destination.parent.mkdir(parents=True, exist_ok=True)

            with open(destination, "wb") as out:
                for tarinfo in archive:
                    if tarinfo.isreg():
                        transforms = self.transforms or Transform.createFromPath(
                            Path(tarinfo.name)
                        )
                        logging.debug("Processing file %s", tarinfo.name)
                        with transforms(archive.fileobject(archive, tarinfo)) as fp:
                            shutil.copyfileobj(fp, out)
