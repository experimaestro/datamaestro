import logging
import shutil
import tarfile
import io
import tempfile
import gzip
import os.path as op, os
import urllib3
from pathlib import Path
from datamaestro.utils import rm_rf
from datamaestro.handlers.transform import Transform
from datamaestro.handlers.download import DownloadHandler


def open_ext(*args, **kwargs):
    """Opens a file according to its extension"""
    name = args[0]
    if name.endswith(".gz"):
        return gzip.open(*args, *kwargs)
    return io.open(*args, **kwargs)

class DatasetPath(DownloadHandler):
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.reference = self.definition["reference"]
        self._path = self.definition.get("path", None)
    
    def path(self, path: Path) -> Path:
        dshandler = self.reference.handler
        rpath = dshandler.destpath
        rpath = dshandler.downloadHandler.path(rpath)    
        if self._path:
            rpath /= self._path
        return rpath 
        
    def download(self, destination):
        pass

class File(DownloadHandler):
    """Single file"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]

    def files(self, destpath, hint: str=None):
        filetype = self.definition.get("type")
        
        if filetype:
            return self.repository.findhandler("files", filetype["__handler__"])(self.path(destpath), filetype)
        return destpath

    def path(self, path: Path, hint: str=None) -> Path:
        """Returns the destination path"""
        p = urllib3.util.parse_url(self.url)
        name = self.definition.get("name", None)
        if not name:
            name = Path(p.path).name
        return path.joinpath(name)

    def download(self, destination):
        logging.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download (cache)
        file = self.dataset.downloadURL(self.url)

        # Transform if need be
        if "transforms" in self.definition:
            logging.info("Transforming file")
            transformer = Transform.create(self.repository, self.definition["transforms"])
            tfile = tempfile.NamedTemporaryFile(delete=True, mode="wb")
            with file.path.open(mode="rb") as r:
                stream = transformer(r)
                while True:
                    b = stream.read(1024)
                    if not b:
                        break
                    tfile.write(b)
            file.delete = True
            file = tfile

        # Move
        file.delete = False
        shutil.move(file.path, destination)
        logging.info("Created file %s" % destination)
        try:
            file.close()
        except:
            pass


class Archive(DownloadHandler):
    """Concatenate all files in an archive"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]
        self.gzip = self.url.endswith(".gz")

    def path(self, path: Path) -> Path:
        """Returns the destination path"""
        p = urllib3.util.parse_url(self.url)
        return path.joinpath(Path(p.path).name)

    def download(self, destination):
        tmpdir = None
        try:
            transformer = None
            if "transforms" in self.definition:
                transformer = Transform.create(self.repository, self.definition["transforms"])

            # Temporary directory
            tmpdir = tempfile.mkdtemp()
            dlfile = self.dataset.downloadURL(self.url)

            d = "%s/all" % tmpdir
            tarfile.open(dlfile.path).extractall(path="%s/all" % tmpdir)
            outfilename = "%s/qrels" % tmpdir #FIXME: seems specific to a dataset
            
            with open(outfilename, 'wb') as f_out:
                for aPath in (os.path.join(d, f) for f in os.listdir(d)):
                    logging.info("Reading %s" % aPath)
                    with open_ext(aPath, "rb") as gzf:
                        f_out.write(gzf.read())

            if transformer:
                tname = outfilename + ".filtered"
                with open(outfilename, mode="rb") as r, open(tname, mode="wb") as w:
                    stream = transformer(r)
                    while True:
                        b = stream.read(1024)
                        if not b:
                            break
                        w.write(b)
                outfilename = tname
                            
            # Move in place
            shutil.move(outfilename, destination)
            dlfile.discard()
            logging.info("Created file %s" % destination)
        finally:
            if not tmpdir is None:
                rm_rf(tmpdir)

