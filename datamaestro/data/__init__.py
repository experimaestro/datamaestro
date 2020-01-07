import logging
from pathlib import Path
from datamaestro.definitions import data, argument

@argument("id", type=str, help="The unique dataset ID")
@data()
class Base:  pass

@data()
class Generic(Base): 
    def __init__(self, **kwargs):
        logging.warning("Generic should be avoided")
        super().__init__()
        for key, value in kwargs.items():
            object.__setattr__(self, key, value)

@argument("path", type=Path)
@data()
class File(Base): 
    """A data file"""
    def open(self, mode):
        return self.path.open(mode)
