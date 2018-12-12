from datamaestro.handlers.download import DownloadHandler
from datamaestro import Dataset
import logging

class Simple(DownloadHandler):
    """Download multiple files or directories"""
    def __init__(self, dataset: Dataset, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition["list"]

    def download(self, destination):
        logging.info("Downloading %d items", len(self.list))
        for key, value in self.list.items():
            handler = DownloadHandler.find(self.dataset, value)
            destpath = handler.resolve(destination)
            if destpath.exists():
                logging.info("File already downloaded [%s]", destpath)
            else:
                handler.download(destpath)
