import logging
from pathlib import Path

from datamaestro import Dataset
from datamaestro.handlers.download import DownloadHandler


class List(DownloadHandler):
    """Download multiple files or directories given by a list"""
    def __init__(self, dataset: Dataset, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition

    def download(self, destination):
        logging.info("Downloading %d items", len(self.list))
        for key, value in self.list.items():
            handler = DownloadHandler.find(self.dataset, value)
            destpath = handler.path(destination, key)
            if destpath.exists():
                logging.info("File already downloaded [%s]", destpath)
            else:
                handler.download(destpath)

    def files(self, destpath):
        """Set the list of files"""
        r =  {}
        for key, value in self.list.items():
            if key != "__handler__":
                handler = DownloadHandler.find(self.dataset, value)
                r[key] = handler.files(destpath)
        return r
