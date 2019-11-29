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
from datamaestro.transform import Transform
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
    """Single file"""
    def __init__(self, varname: str, url: str, name :str=None, transforms=None):
        super().__init__(varname)
  
        self.url = url

        # Infer name and 
        p = urllib3.util.parse_url(self.url)
        path = Path(p.path)

        self.name = name
        
        if transforms is None:
            self.name, self.transformer = Transform.createFromPath(path)
        else:
            self.transformer = Transform.create(self.repository, self.definition["transforms"])
        
        if self.name is None:
            self.name = p.name
            

    def _download(self, destination):
        logging.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download (cache)
        with self.context.downloadURL(self.url) as file:
            # Transform if need be
            if self.transformer:
                logging.info("Transforming file")
                with self.transformer(file.path.open("rb")) as stream, destination.open("wb") as out:
                    shutil.copyfileobj(stream, out)
            else:
                logging.info("Keeping original downloaded file %s", file.path)
                (shutil.copy if file.keep else shutil.move)(file.path, destination)


        logging.info("Created file %s" % destination)


class Concat(SingleDownload):
    """Concatenate all files in an archive"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]
        self.gzip = self.url.endswith(".gz")

    def path(self, path: Path) -> Path:
        """Returns the destination path"""
        p = urllib3.util.parse_url(self.url)
        return path / Path(p.path).stem

    def _download(self, destination):
        with NamedTemporaryFile("wb") as f,  self.dataset.downloadURL(self.url) as dl, tarfile.open(dl.path) as archive:
            if "transforms" in self.definition:
                transformer = Transform.create(self.repository, self.definition["transforms"]) 
            else:
                transformer = lambda x: x

            destination.parent.mkdir(parents=True, exist_ok=True)
            with open(destination, "wb") as out:
                for tarinfo in archive:
                    if tarinfo.isreg():
                        logging.debug("Processing file %s", tarinfo.name)
                        with transformer(archive.fileobject(archive, tarinfo)) as fp:
                            shutil.copyfileobj(fp, out)
