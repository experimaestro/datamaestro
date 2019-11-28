import logging
from pathlib import Path
import os

from datamaestro import DatasetDefinition
from datamaestro.download import DownloadHandler


class List(DownloadHandler):
    """Download multiple files or directories given by a list"""
    def __init__(self, dataset: DatasetDefinition, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition

    def download(self, destination):
        logging.info("Downloading %d items", len(self.list))
        for key, value in self.list.items():
            if not key.startswith("__"):
                handler = DownloadHandler.find(self.dataset, value)
                destpath = handler.path(destination, key)
                handler.download(destpath)

    def files(self, destpath):
        """Set the list of files"""
        r = {}
        for key, value in self.list.items():
            if not key.startswith("__"):
                handler = DownloadHandler.find(self.dataset, value)
                r[key] = handler.files(destpath)
        return r


class Datasets(DownloadHandler):
    """Use links to dataset files"""
    def __init__(self, dataset: DatasetDefinition, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition

    def download(self, destination):
        for key, value in self.list.items():
            if not key.startswith("__"):
                files = value.prepare().files
                if isinstance(files, Path):
                    if not (destination / key).exists():
                        destination.mkdir(exist_ok=True, parents=True)
                        os.symlink(files, destination / key)
                elif len(files) > 1:
                    raise NotImplementedError()

    def files(self, destpath):
        """Set the list of files"""
        return destpath
