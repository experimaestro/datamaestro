import logging
from datasets.handlers.download import DownloadHandler
from pathlib import Path
import zipfile
import shutil

class Zip(DownloadHandler):
    """Single file"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]

    def download(self, destination: Path):
        logging.info("Downloading %s into %s", self.url, destination)

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmpdestination = destination.with_suffix(".tmp")
        if tmpdestination.exists():
            logging.warn("Removing temporary directory %s", tmpdestination)
            shutil.rmtree(tmpdestination)

        file = self.context.download(self.url)
        
        logging.info("Unzipping file")
        with zipfile.ZipFile(file.path) as zip:
            zip.extractall(tmpdestination)

        for ix, path in enumerate(tmpdestination.iterdir()):
            if ix > 1: break
        
        # Just one file/folder: move
        if ix == 0 and path.is_dir():
            logging.info("Moving single directory into destination")
            shutil.move(path, destination)
            shutil.rmtree(tmpdestination)
        else:
            shutil.move(tmpdestination, destination)

