import logging
from pathlib import Path
from datamaestro.definitions import data, argument


def documentation(method):
    """Indicates that the method should be included in the documentation"""
    method.__datamaestro_doc__ = True
    return method


@argument("id", type=str, help="The unique dataset ID", required=False)
@data()
class Base:
    pass


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
