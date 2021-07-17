import logging
import os
from datamaestro.download import Download
from datamaestro.utils import deprecated
from datamaestro.definitions import AbstractDataset
from typing import List
from datamaestro.download import Download
from datamaestro.context import ResolvablePath
from pathlib import Path
import os
import logging


class links(Download):
    def __init__(self, varname: str, **links: List[AbstractDataset]):
        """Link with another dataset path

        Args:
            varname: The name of the variable when defining the dataset
            links: A list of
        """
        super().__init__(varname)
        self.links = links

    @property
    def path(self):
        return self.definition.datapath

    def prepare(self):
        return self.path

    def download(self, force=False):
        self.path.mkdir(exist_ok=True, parents=True)
        for key, value in self.links.items():
            value.download(force)

            path = value()
            dest = self.path / key

            if not dest.exists():
                if dest.is_symlink():
                    logging.info("Removing dandling symlink %s", dest)
                    dest.unlink()
                os.symlink(path, dest)


# Deprecated
Links = deprecated("Use @links instead of @Links", links)


class linkpath(Download):
    def __init__(self, varname: str, proposals):
        """Link to a folder

        Args:
            varname: Name of the variable
            proposals: List of potential paths
        """
        super().__init__(varname)
        self.proposals = proposals

    def prepare(self):
        return self.path

    @property
    def path(self):
        return self.definition.datapath / self.varname

    def download(self, destination):
        if self.check(self.path):
            return

        if self.path.is_symlink():
            logging.warning("Removing dandling symlink %s", self.path)
            self.path.unlink()

        path = None

        for searchpath in self.proposals:
            logging.info("Trying path %s", searchpath)
            try:
                path = ResolvablePath.resolve(self.context, searchpath)
                if self.check(path):
                    break
                logging.info("Path %s not found", path)
            except KeyError:
                logging.info("Could not expand path %s", searchpath)

        # Ask the user
        while path is None or not self.check(path):
            path = Path(input("Path to %s: " % self.varname))
        assert path.name

        logging.debug("Linking %s to %s", path, self.path)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        os.symlink(path, self.path)


class linkfolder(linkpath):
    def check(self, path):
        return path.is_dir()

    def __init__(self, varname: str, proposals):
        """Link to a folder

        Args:
            varname: Name of the variable
            proposals: List of potential paths
        """
        super().__init__(varname, proposals)


class linkfile(linkpath):
    def __init__(self, varname: str, proposals):
        """Link to a file

        Args:
            varname: Name of the variable
            proposals: List of potential paths
        """
        super().__init__(varname, proposals)

    def check(self, path):
        print("Checking", path, path.is_file())
        return path.is_file()
