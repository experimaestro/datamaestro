"""Multiple download resources (legacy).

Note: This module uses a legacy API pattern and needs deeper refactoring.
The List and Datasets classes use an older constructor signature that
differs from the modern Resource interface.
"""

import logging
import os
import warnings
from pathlib import Path

from datamaestro.definitions import AbstractDataset
from datamaestro.download import Download

warnings.warn(
    "datamaestro.download.multiple uses a legacy API. "
    "Consider migrating to class-attribute resource definitions.",
    DeprecationWarning,
    stacklevel=2,
)


class List(Download):
    """Download multiple files or directories given by a list.

    Legacy: uses old-style constructor API.
    """

    def __init__(self, dataset: AbstractDataset, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition

    def download(self, destination):
        logging.info("Downloading %d items", len(self.list))
        for key, value in self.list.items():
            if not key.startswith("__"):
                handler = Download.find(self.dataset, value)
                destpath = handler.path(destination, key)
                handler.download(destpath)

    def files(self, destpath):
        """Set the list of files"""
        r = {}
        for key, value in self.list.items():
            if not key.startswith("__"):
                handler = Download.find(self.dataset, value)
                r[key] = handler.files(destpath)
        return r


class Datasets(Download):
    """Use links to dataset files.

    Legacy: uses old-style constructor API.
    """

    def __init__(self, dataset: AbstractDataset, definition: object):
        super().__init__(dataset, definition)
        self.list = self.definition

    def download(self, destination):
        destination.mkdir(exist_ok=True, parents=True)

        for key, value in self.list.items():
            if not key.startswith("__"):
                files = value.prepare().files

                if isinstance(files, Path):
                    if not files.is_dir():
                        raise AssertionError(
                            "Dataset path is not a directory: %s",
                            files,
                        )
                    path = destination / key
                    if not path.exists():
                        if path.is_symlink():
                            logging.warning("Path %s is symlink", path)
                        else:
                            os.symlink(files, path)
                elif len(files) > 1:
                    raise NotImplementedError()

    def files(self, destpath):
        """Set the list of files"""
        return destpath
