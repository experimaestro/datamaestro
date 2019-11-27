from pathlib import Path
from datamaestro.definitions import Repository
from datamaestro import DatasetDefinition

class DownloadHandler:
    """
    Base class for all download handlers
    """
    def __init__(self, dataset: DatasetDefinition, definition: object):
        self.dataset = dataset
        self.repository = dataset.repository
        self.context = self.repository.context
        self.definition = definition

    def path(self, destination: Path, hint: str=None):
        """Returns the destination - by default, preserves the path"""
        if hint:
            return destination / hint
        return destination

    def download(self, destination: Path):
        """Downloads the content and place it in the specified destination"""
        raise NotImplementedError()

    def files(self, destpath):
        """Get the list of files
        
        By default, retures the generic handler for the destination path
        """
        filetype = self.definition.get("type")
        if filetype:
            return self.repository.findhandler_of("files", filetype)(self.path(destpath), filetype)
        return destpath

    @staticmethod
    def find(datadef, definition) -> "DownloadHandler":
        handler = definition.get("__handler__", definition.get("handler", None))
        if handler is None:
            raise Exception("No handler defined for %s" % definition)
        return datadef.repository.findhandler("download", handler)(datadef, definition)