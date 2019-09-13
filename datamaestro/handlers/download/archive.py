import logging
from datamaestro.handlers.download import DownloadHandler
from pathlib import Path
import zipfile
import shutil
import urllib3
import tarfile

class Zip(DownloadHandler):
    """ZIP Archive handler"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]

    def resolve(self, path: Path) -> Path:
        """Returns the destination path"""
        p = urllib3.util.parse_url(self.url)
        return path.joinpath(Path(p.path).name)

    def download(self, destination: Path):
        logging.info("Downloading %s into %s", self.url, destination)

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmpdestination = destination.with_suffix(".tmp")
        if tmpdestination.exists():
            logging.warn("Removing temporary directory %s", tmpdestination)
            shutil.rmtree(tmpdestination)

        file = self.dataset.downloadURL(self.url)
        
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



class Tar(DownloadHandler):
    """TAR archive handler"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]

    def resolve(self, path: Path) -> Path:
        """Returns the destination path"""
        p = urllib3.util.parse_url(self.url)
        return path.joinpath(Path(p.path).name)

    def download(self, destination: Path):
        logging.info("Downloading %s into %s", self.url, destination)
        
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmpdestination = destination.with_suffix(".tmp")
        if tmpdestination.exists():
            logging.warn("Removing temporary directory %s", tmpdestination)
            shutil.rmtree(tmpdestination)

        file = self.dataset.downloadURL(self.url)
        
        logging.info("Unarchiving file")
        with tarfile.TarFile.open(file.path, mode="r:*") as tar:
            tar.extractall(tmpdestination)

        for ix, path in enumerate(tmpdestination.iterdir()):
            if ix > 1: break
        
        # Just one file/folder: move
        if ix == 0 and path.is_dir():
            logging.info("Moving single directory into destination")
            shutil.move(path, destination)
            shutil.rmtree(tmpdestination)
        else:
            shutil.move(tmpdestination, destination)

