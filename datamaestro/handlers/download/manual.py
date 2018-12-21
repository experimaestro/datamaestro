from datamaestro.handlers.download import DownloadHandler
from pathlib import Path
import os
import logging

class DownloadPath(DownloadHandler):
    """Just asks for the location of the file and link it"""

    def download(self, destination):
        path = None
        while path is None or not path.is_file():
            path = Path(input("Path to %s: " % self.definition["name"]))

        logging.debug("Linking %s to %s", path, destination)
        os.link(path, destination)        