import logging
import shutil
import tarfile
import io
import tempfile
import gzip
import os.path as op, os
import urllib3
from pathlib import Path
from tempfile import NamedTemporaryFile

from datamaestro.utils import rm_rf
from datamaestro.stream import Transform
from datamaestro.download import Download


def open_ext(*args, **kwargs):
    """Opens a file according to its extension"""
    name = args[0]
    if name.endswith(".gz"):
        return gzip.open(*args, *kwargs)
    return io.open(*args, **kwargs)


class SingleDownload(Download):
    @property
    def path(self):
        return self.definition.destpath / self.name

    def download(self, force=False):
        if not self.path.is_file():
            self._download(self.path)
        

class DownloadFile(SingleDownload):
    """Downloads a single file given by a URL"""
    
    def __init__(self, varname: str, url: str, name :str=None, transforms=None):
        super().__init__(varname)
  
        self.url = url

        # Infer name and 
        p = urllib3.util.parse_url(self.url)
        path = Path(p.path)

        self.transforms = transforms if transforms else Transform.createFromPath(path)
        self.name = Path(name) if name else self.transforms.path(Path(p.path))
            

    def _download(self, destination):
        logging.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download (cache)
        with self.context.downloadURL(self.url) as file:
            # Transform if need be
            if self.transforms:
                logging.info("Transforming file")
                with self.transforms(file.path.open("rb")) as stream, destination.open("wb") as out:
                    shutil.copyfileobj(stream, out)
            else:
                logging.info("Keeping original downloaded file %s", file.path)
                (shutil.copy if file.keep else shutil.move)(file.path, destination)


        logging.info("Created file %s" % destination)


class ConcatDownload(SingleDownload):
    """Concatenate all files in an archive"""

    def __init__(self, varname: str, url: str, name :str=None, transforms=None):
        super().__init__(varname)
  
        self.url = url

        # Infer name and 
        p = urllib3.util.parse_url(self.url)
        path = Path(p.path)

        self.transforms = transforms if transforms else Transform.createFromPath(path)
        self.name = Path(name) if name else self.transforms.path(Path(p.path))

    def _download(self, destination):
        with NamedTemporaryFile("wb") as f,  self.dataset.downloadURL(self.url) as dl, tarfile.open(dl.path) as archive:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with open(destination, "wb") as out:
                for tarinfo in archive:
                    if tarinfo.isreg():
                        logging.debug("Processing file %s", tarinfo.name)
                        with self.transforms(archive.fileobject(archive, tarinfo)) as fp:
                            shutil.copyfileobj(fp, out)
