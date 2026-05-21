import logging
from pathlib import Path
from typing import Any, Dict
from experimaestro import Prepare, Param, Meta
from datamaestro.definitions import AbstractDataset


class Base(Prepare):
    """Base object for all data types.

    Inherits from :class:`experimaestro.Prepare`: any Task that references
    a dataset config in its params will trigger ``self.prepare()`` (i.e.
    a download) before the task runs. Downloads are idempotent on a warm
    cache, so it is safe to keep declaring ``prepare_dataset(...)`` in
    experiment scripts that may run multiple times.
    """

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
        """Download the dataset (idempotent on a warm cache).

        Called by experimaestro as an in-memory dependency before any task
        that references this dataset runs. Also safe to call directly.
        """
        self.__datamaestro_dataset__.download()
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
