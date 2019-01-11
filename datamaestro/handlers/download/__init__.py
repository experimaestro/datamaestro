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
        """Returns the destination - by default, a preserves the path"""
        return destination

    def download(self, destination: Path):
        """Downloads the content and place it in the specified destination"""
        raise NotImplementedError()

    def updateDatasetInformation(self, destpath: Path, info: dict):
        """Update the dataset information dictionary with file-based information"""
        pass

    @staticmethod
    def find(dataset, definition):
        return dataset.repository.findhandler("download", definition["handler"])(dataset, definition)