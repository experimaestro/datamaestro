import logging
from pathlib import Path
from datamaestro.definitions import AbstractDataset, argument, Param
from experimaestro import Config
from experimaestro import documentation  # noqa: F401


class Base(Config):
    """Base object for all data types"""

    id: Param[str]
    """The unique dataset ID"""

    __datamaestro_dataset__: AbstractDataset

    def download(self):
        """Download the dataset"""
        self.__datamaestro_dataset__.download()


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


class File(Base):
    """A data file"""

    path: Param[Path]
    """The path of the file"""

    def open(self, mode):
        return self.path.open(mode)


@argument("path", type=Path)
class Folder(Base):
    """A data folder"""

    def open(self, mode):
        return self.path.open(mode)
