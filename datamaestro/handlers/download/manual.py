from datamaestro.handlers.download import DownloadHandler
from pathlib import Path
import os
import logging

class DownloadPath(DownloadHandler):
    """Just asks for the location of the file and link it"""

    def download(self, destination):
        if destination.is_dir(): 
            return
        
        name = self.definition["name"]
        path = None

        # Check a folder given by an environment variable
        envpath = self.definition.get("environ", None)
        envpath = os.environ.get(envpath) if envpath else None

        if envpath:
            path = Path(envpath) / name
            if not path.is_dir():
                logging.warning("Folder %s not found within %s", name, envpath)
        
        # Ask the user
        while path is None or not path.is_dir():
            path = Path(input("Path to %s: " % name))

        logging.debug("Linking %s to %s", path, destination)
        os.symlink(path, destination)
