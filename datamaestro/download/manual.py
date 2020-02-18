from datamaestro.download import Download
from pathlib import Path
import os
import logging


def envreplace(value):
    """Replace %...% by the value of the environment variable"""

    def process(var):
        return os.environ[var.group(1)]

    return re.sub(r"%([^%]+)%", process, value)


class linkfolder(Download):
    """Just asks for the location of the file and link it"""

    def __init__(self, varname: str, proposals):
        super().__init__(varname)
        self.proposals = proposals

    def prepare(self):
        return self.path

    @property
    def path(self):
        return self.definition.datapath / self.varname

    def download(self, destination):
        if self.path.is_dir():
            return

        if self.path.is_symlink():
            logging.warning("Removing dandling symlink %s", self.path)
            self.path.unlink()

        path = None

        # Check a folder given by an environment variable
        for searchpath in self.proposals:
            logging.info("Trying path %s", searchpath)
            try:
                path = Path(self.context.datafolder_process(searchpath))
                if path.is_dir():
                    break
                logging.info("Folder %s not found", path)
            except KeyError:
                logging.info("Could not expand path %s", searchpath)

        # Ask the user
        while path is None or not path.is_dir():
            path = Path(input("Path to %s: " % self.varname))
        assert path.name

        logging.debug("Linking %s to %s", path, self.path)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        os.symlink(path, self.path)
