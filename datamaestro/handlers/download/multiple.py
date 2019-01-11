import logging
from pathlib import Path

from datamaestro import Dataset
from datamaestro.handlers.download import DownloadHandler


class List(DownloadHandler):
    """Download multiple files or directories given by a list"""
    def __init__(self, dataset: Dataset, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition["list"]

    def download(self, destination):
        logging.info("Downloading %d items", len(self.list))
        for key, value in self.list.items():
            handler = DownloadHandler.find(self.dataset, value)
            destpath = handler.path(destination)
            if destpath.exists():
                logging.info("File already downloaded [%s]", destpath)
            else:
                handler.download(destpath)

    def updateDatasetInformation(self, destpath: Path, info: dict):
        for key, value in self.list.items():
            handler = DownloadHandler.find(self.dataset, value)
            info[key] = handler.path(destpath)
