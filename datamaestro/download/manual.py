from datamaestro.download import Download
from pathlib import Path
import os
import re
import logging

def envreplace(value):
    """Replace %...% by the value of the environment variable"""
    def process(var):
        return os.environ[var.group(1)]
    return re.sub(r"%([^%]+)%", process, value)


class LinkFolder(Download):
    """Just asks for the location of the file and link it"""
    def __init__(self, varname: str, name: str, proposals):
        super().__init__(varname)
        self.name = name
        self.proposals = proposals


    @property
    def path(self):
        return self.definition.destpath / self.name

    def download(self, destination):
        if self.path.is_dir(): 
            return
        if self.path.is_symlink():
            raise AssertionError("Symlink exists but does not point to a directory")
            
        path = None

        # Check a folder given by an environment variable
        for searchpath in self.proposals:
            logging.debug("Trying path %s", searchpath)
            try:
                dir = envreplace(searchpath)
                path = Path(dir) / self.name
                if path.is_dir():
                    break
                logging.info("Folder %s not found within %s", self.name, dir)
            except KeyError:
                logging.info("Could not expand path %s", searchpath)

        # Ask the user
        while path is None or not path.is_dir():
            path = Path(input("Path to %s: " % self.name))
        assert path.name

        logging.debug("Linking %s to %s", path, self.path)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        os.symlink(path, self.path)
