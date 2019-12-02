"""Machine learning generic data formats"""
from typing import List
from pathlib import Path
from . import Generic

class Supervised(Generic): 
    """"""
    pass

class FolderBased(Generic):
    """Data where each class is a folder"""
    def __init__(self, path:Path, classes:List[str]=None):
        """
        :params folders: list of folders
        """
        self.path = path

        if not classes:
            classes = [s.name for s in self.path.iterdir() if s.is_dir()]

        self.classes = {c: self.path / c for c in classes}