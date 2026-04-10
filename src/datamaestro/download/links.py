"""Link-based resources.

Provides resources that create symlinks to other datasets or
user-specified paths.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
from pathlib import Path
from typing import List, Optional

from datamaestro.context import ResolvablePath
from datamaestro.definitions import AbstractDataset
from datamaestro.download import LocalResourceMixin, Resource
from datamaestro.utils import deprecated

logger = logging.getLogger(__name__)


class GlobChecker:
    """Verifies folder contents by computing a combined MD5 over matching files.

    Files matching the glob pattern are sorted by name, each file's MD5 is
    computed, and then the MD5 of the concatenated hex digests gives the
    overall checksum.

    If ``md5`` is ``None``, the computed checksum is logged so the user
    can record it for future verification.
    """

    def __init__(self, glob: str, md5: Optional[str] = None):
        self.glob = glob
        self.md5 = md5

    @staticmethod
    def _file_md5(path: Path) -> str:
        """Compute MD5 of a file, decompressing .gz files first."""
        data = path.read_bytes()
        if path.suffix == ".gz":
            data = gzip.decompress(data)
        return hashlib.md5(data).hexdigest()

    def compute(self, path: Path) -> Optional[str]:
        """Compute the combined MD5 for files matching the glob under *path*."""
        files = sorted(path.glob(self.glob))
        if not files:
            return None
        combined = hashlib.md5()
        for f in files:
            if f.is_file():
                combined.update(self._file_md5(f).encode())
        return combined.hexdigest()

    def check(self, path: Path) -> bool:
        digest = self.compute(path)
        if digest is None:
            raise FileNotFoundError(f"No files matching '{self.glob}' in {path}")
        if self.md5 is None:
            logger.info(
                "GlobChecker(%s): computed md5 = %s for %s", self.glob, digest, path
            )
            return True
        if digest != self.md5:
            logger.error(
                "GlobChecker(%s): md5 mismatch for %s: expected %s, got %s",
                self.glob,
                path,
                self.md5,
                digest,
            )
            return False
        return True


class links(LocalResourceMixin, Resource):
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
                path = wrapper.datapath
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


class linkpath(LocalResourceMixin, Resource):
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
        if self._check_path(self.path):
            return

        if self.path.is_symlink():
            logger.warning("Removing dangling symlink %s", self.path)
            self.path.unlink()

        path = None

        for searchpath in self.proposals:
            logger.info("Trying path %s", searchpath)
            try:
                path = ResolvablePath.resolve(self.context, searchpath)
                if self._check_path(path):
                    break
                logger.info("Path %s not found", path)
            except KeyError:
                logger.info("Could not expand path %s", searchpath)

        if path is None or not self._check_path(path):
            raise FileNotFoundError("No valid path found for '%s'" % self.name)

        logger.debug("Linking %s to %s", path, self.path)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        os.symlink(path, self.path)

    def _check_path(self, path):
        raise NotImplementedError()


class linkfolder(linkpath):
    """Link to a folder.

    Usage as class attribute::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = linkfolder("data", proposals=[...])

    An optional ``checker`` (e.g. :class:`GlobChecker`) can be provided to
    verify the folder contents after linking::

        DATA = linkfolder("data", proposals=[...],
                          checker=GlobChecker("FB*", "a1b2c3..."))
    """

    def __init__(
        self,
        varname: str,
        proposals,
        *,
        transient: bool = False,
        checker: Optional[GlobChecker] = None,
    ):
        super().__init__(varname, proposals, transient=transient)
        self.checker = checker

    def _check_path(self, path):
        if not path.is_dir():
            return False
        if self.checker is not None:
            return self.checker.check(path)
        return True


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

    def _check_path(self, path):
        logger.debug("Checking %s (exists: %s)", path, path.is_file())
        return path.is_file()
