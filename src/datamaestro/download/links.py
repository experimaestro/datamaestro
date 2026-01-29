"""Link-based resources.

Provides resources that create symlinks to other datasets or
user-specified paths.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List

from datamaestro.context import ResolvablePath
from datamaestro.definitions import AbstractDataset
from datamaestro.download import Resource
from datamaestro.utils import deprecated

logger = logging.getLogger(__name__)


class links(Resource):
    """Link with another dataset path.

    Usage as class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = links("data", ref1=other_dataset1)

    Usage as decorator (deprecated)::

        @links("data", ref1=other_dataset1)
        @dataset(Base)
        def my_dataset(data): ...
    """

    def __init__(
        self,
        varname: str,
        *,
        transient: bool = False,
        **link_targets: List[AbstractDataset],
    ):
        super().__init__(varname=varname, transient=transient)
        self.links = link_targets

    @property
    def path(self):
        return self.dataset.datapath

    def prepare(self):
        return self.path

    def download(self, force=False):
        self.path.mkdir(exist_ok=True, parents=True)
        for key, value in self.links.items():
            # Resolve class-based datasets
            if hasattr(value, "__dataset__"):
                wrapper = value.__dataset__
                wrapper.download(force)
                path = wrapper()
            elif hasattr(value, "download"):
                value.download(force)
                path = value()
            else:
                path = value  # Already a path

            dest = self.path / key

            if not dest.exists():
                if dest.is_symlink():
                    logger.info("Removing dangling symlink %s", dest)
                    dest.unlink()
                os.symlink(path, dest)

    def has_files(self):
        return False


# Deprecated
Links = deprecated("Use @links instead of @Links", links)


class linkpath(Resource):
    """Link to a path selected from proposals.

    Usage as class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = linkpath("data", proposals=[...])
    """

    def __init__(
        self,
        varname: str,
        proposals,
        *,
        transient: bool = False,
    ):
        super().__init__(varname=varname, transient=transient)
        self.proposals = proposals

    def prepare(self):
        return self.path

    @property
    def path(self):
        return self.dataset.datapath / self.name

    def download(self, force=False):
        if self.check(self.path):
            return

        if self.path.is_symlink():
            logger.warning("Removing dangling symlink %s", self.path)
            self.path.unlink()

        path = None

        for searchpath in self.proposals:
            logger.info("Trying path %s", searchpath)
            try:
                path = ResolvablePath.resolve(self.context, searchpath)
                if self.check(path):
                    break
                logger.info("Path %s not found", path)
            except KeyError:
                logger.info("Could not expand path %s", searchpath)

        # Ask the user
        while path is None or not self.check(path):
            path = Path(input("Path to %s: " % self.name))
        assert path.name

        logger.debug("Linking %s to %s", path, self.path)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        os.symlink(path, self.path)

    def check(self, path):
        raise NotImplementedError()


class linkfolder(linkpath):
    """Link to a folder.

    Usage as class attribute::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = linkfolder("data", proposals=[...])
    """

    def __init__(
        self,
        varname: str,
        proposals,
        *,
        transient: bool = False,
    ):
        super().__init__(varname, proposals, transient=transient)

    def check(self, path):
        return path.is_dir()


class linkfile(linkpath):
    """Link to a file.

    Usage as class attribute::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = linkfile("data", proposals=[...])
    """

    def __init__(
        self,
        varname: str,
        proposals,
        *,
        transient: bool = False,
    ):
        super().__init__(varname, proposals, transient=transient)

    def check(self, path):
        logger.debug("Checking %s (exists: %s)", path, path.is_file())
        return path.is_file()
