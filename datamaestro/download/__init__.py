from pathlib import Path
from datamaestro.definitions import DataAnnotation

class Download(DataAnnotation):
    """
    Base class for all download handlers
    """
    
    def __init__(self, varname: str):
        self.varname = varname

    def annotate(self):
        # Register has a resource download
        if self.varname in self.definition.resources:
            raise AssertionError("Name %s already declared as a resource", self.varname)

        self.definition.resources[self.varname] = self

    def download(self, force=False):
        """Downloads the content"""
        raise NotImplementedError()
