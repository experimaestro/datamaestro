from pathlib import Path
from datamaestro.data import Repository
from datamaestro import Dataset

class DownloadHandler:
    """
    Base class for all download handlers
    """
    def __init__(self, dataset: Dataset, definition: object):
        self.dataset = dataset
        self.repository = dataset.repository
        self.context = self.repository.context
        self.definition = definition

    def path(self, destination: Path):
        """Returns the destination - by default, a folder with the same name"""
        return destination

    @staticmethod
    def find(dataset, definition):
        return dataset.repository.findhandler("download", definition["handler"])(dataset, definition)