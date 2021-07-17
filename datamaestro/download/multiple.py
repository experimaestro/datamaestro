import logging
from pathlib import Path
import os

from datamaestro import AbstractDataset
from datamaestro.download import Download


class List(Download):
    """Download multiple files or directories given by a list"""

    def __init__(self, dataset: AbstractDataset, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition

    def download(self, destination):
        logging.info("Downloading %d items", len(self.list))
        for key, value in self.list.items():
            if not key.startswith("__"):
                handler = Download.find(self.dataset, value)
                destpath = handler.path(destination, key)
                handler.download(destpath)

    def files(self, destpath):
        """Set the list of files"""
        r = {}
        for key, value in self.list.items():
            if not key.startswith("__"):
                handler = Download.find(self.dataset, value)
                r[key] = handler.files(destpath)
        return r


class Datasets(Download):
    """Use links to dataset files"""

    def __init__(self, dataset: AbstractDataset, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition

    def download(self, destination):
        destination.mkdir(exist_ok=True, parents=True)

        for key, value in self.list.items():
            if not key.startswith("__"):
                files = value.prepare().files

                if isinstance(files, Path):
                    if not files.is_dir():
                        raise AssertionError(
                            "Dataset path is not a directory: %s", files
                        )
                    path = destination / key
                    if not path.exists():
                        if path.is_symlink():
                            logging.warning("Path %s is symlink", path)
                        else:
                            os.symlink(files, path)
                elif len(files) > 1:
                    raise NotImplementedError()

    def files(self, destpath):
        """Set the list of files"""
        return destpath
