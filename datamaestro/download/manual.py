from datamaestro.download import DownloadHandler
from pathlib import Path
import os
import re
import logging

def envreplace(value):
    """Replace %...% by the value of the environment variable"""
    def process(var):
        return os.environ[var.group(1)]
    return re.sub(r"%([^%]+)%", process, value)


class DownloadPath(DownloadHandler):
    """Just asks for the location of the file and link it"""

    def download(self, destination):
        if destination.is_dir(): 
            return
        
        name = self.definition["name"]
        path = None

        # Check a folder given by an environment variable
        for searchpath in self.definition.get("search", []):
            logging.debug("Trying path %s", searchpath)
            try:
                dir = envreplace(searchpath)
                path = Path(dir) / name
                if path.is_dir():
                    break
                logging.info("Folder %s not found within %s", name, dir)
            except KeyError:
                logging.info("Could not expand path %s", searchpath)

        # Ask the user
        while path is None or not path.is_dir():
            path = Path(input("Path to %s: " % name))

        logging.debug("Linking %s to %s", path, destination)
        destination.parent.mkdir(exist_ok=True, parents=True)
        os.symlink(path, destination)
