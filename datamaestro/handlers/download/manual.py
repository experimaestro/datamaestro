from datamaestro.handlers.download import DownloadHandler
from pathlib import Path
import os
import logging

class DownloadPath(DownloadHandler):
    """Just asks for the location of the file and link it"""

    def download(self, destination):
        if destination.is_dir(): 
            return
            
        path = None
        while path is None or not path.is_dir():
            path = Path(input("Path to %s: " % self.definition["name"]))

        logging.debug("Linking %s to %s", path, destination)
        os.symlink(path, destination)
