import logging
from pathlib import Path
from datamaestro.definitions import data, argument, Param
from experimaestro import Config
from experimaestro import documentation  # noqa: F401


@argument("id", type=str, help="The unique dataset ID", required=False)
@data()
class Base(Config):
    pass


@data()
class Generic(Base):
    """Generic dataset

    This allows to set any value, but should only be used
    as a placeholder
    """

    def __init__(self, **kwargs):
        logging.warning("Generic should be avoided")
        super().__init__()
        for key, value in kwargs.items():
            object.__setattr__(self, key, value)


@data()
class File(Base):
    """A data file"""

    path: Param[Path]

    def open(self, mode):
        return self.path.open(mode)


@argument("path", type=Path)
@data()
class Folder(Base):
    """A data folder"""

    def open(self, mode):
        return self.path.open(mode)
