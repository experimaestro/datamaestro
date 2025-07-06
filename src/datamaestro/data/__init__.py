import logging
from pathlib import Path
from typing import Any, Dict
from experimaestro import Config, Param, Meta
from datamaestro.definitions import AbstractDataset


class Base(Config):
    """Base object for all data types"""

    id: Param[str]
    """The unique (sub-)dataset ID"""

    __datamaestro_dataset__: "AbstractDataset"

    def dataset_information(self) -> Dict[str, Any]:
        """Returns document meta-informations"""
        return {
            "id": self.id,
            "name": self.__datamaestro_dataset__.name,
            "description": self.__datamaestro_dataset__.description,
        }

    def download(self):
        """Download the dataset"""
        self.__datamaestro_dataset__.download()

    def prepare(self, *args, **kwargs):
        """Prepare the dataset"""
        self.__datamaestro_dataset__.prepare()
        return self


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

    path: Meta[Path]
    """The path of the file"""

    def open(self, mode):
        return self.path.open(mode)


class Folder(Base):
    """A data folder"""

    path: Meta[Path]

    def open(self, mode):
        return self.path.open(mode)
