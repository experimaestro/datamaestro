import logging
from pathlib import Path
from typing import Any, Dict, Optional
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

    @property
    def __datamaestro_dataset__(self) -> Optional["AbstractDataset"]:
        """The owning dataset wrapper, used to run downloads.

        ``AbstractDataset.prepare`` links the live wrapper here as a plain
        attribute. In the experiment driver the config is used as-is, so
        that live object is returned directly (fast path).

        The link is *not* a registered ``Param``/``Meta``, so it does not
        survive being serialized to a worker (or an explicit ``copy()`` /
        ``clone()`` — see issue #25). When it is missing we rebuild the
        wrapper from the ``id`` param, which *does* survive. A dynamically
        generated config (custom subset / unregistered collection) has no
        registered ``id`` and therefore no wrapper: we return ``None``.
        """
        try:
            return self.__dict__["__datamaestro_dataset__"]
        except KeyError:
            pass

        # ``id`` is a Param: experimaestro raises KeyError when it is unset
        # (dynamically generated config), AttributeError on a bare instance.
        try:
            base_id = self.id
        except (KeyError, AttributeError):
            base_id = None
        if not base_id:
            return None

        try:
            from datamaestro.context import Context

            # Strip any repository / variant suffix (e.g. "id@repo").
            dataset = Context.instance().dataset(base_id.split("@")[0])
        except Exception:
            return None

        self.__dict__["__datamaestro_dataset__"] = dataset
        return dataset

    @__datamaestro_dataset__.setter
    def __datamaestro_dataset__(self, value):
        self.__dict__["__datamaestro_dataset__"] = value

    def dataset_information(self) -> Dict[str, Any]:
        """Returns document meta-informations"""
        ds = self.__datamaestro_dataset__
        return {
            "id": self.id,
            "name": ds.name if ds is not None else "",
            "description": ds.description if ds is not None else "",
        }

    def download(self):
        """Download the dataset (no-op when there is no owning dataset)."""
        ds = self.__datamaestro_dataset__
        if ds is not None:
            ds.download()

    def prepare(self, *args, **kwargs):
        """Download the dataset (idempotent on a warm cache).

        Called by experimaestro as an in-memory dependency before any task
        that references this dataset runs. Also safe to call directly. Does
        nothing when the config has no owning dataset (dynamically generated
        subsets / unregistered collections).
        """
        self.download()
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
