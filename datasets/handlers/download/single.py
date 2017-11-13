import logging
import shutil
import tarfile
import io
import tempfile
import gzip
import urllib.request
import os.path as op, os
from datasets.utils import rm_rf
from datasets.handlers.transform import Transform
from datasets.handlers.download import DownloadHandler


def open_ext(*args, **kwargs):
    """Opens a file according to its extension"""
    name = args[0]
    if name.endswith(".gz"):
        return gzip.open(*args, *kwargs)
    return io.open(*args, **kwargs)

class File(DownloadHandler):
    """Single file"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]

    def download(self, destination):
        logging.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download
        file = tempfile.NamedTemporaryFile(delete=True)
        urllib.request.urlretrieve(self.url, file.name)

        # Transform if need be
        if "transforms" in self.definition:
            logging.info("Transforming file")
            transformer = Transform.create(self.repository, self.definition["transforms"])
            tfile = tempfile.NamedTemporaryFile(delete=True, mode="wb")
            with open(file.name, mode="rb") as r:
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
        shutil.move(file.name, destination)
        logging.info("Created file %s" % destination)
        try:
            file.close()
        except:
            pass
        try:
            gzfile.close()
        except:
            pass


class Archive(DownloadHandler):
    """Concatenate all files in an archive"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]
        self.gzip = self.url.endswith(".gz")

    def download(self, destination):
        tmpdir = None
        try:
            transformer = None
            if "transforms" in self.definition:
                transformer = Transform.create(self.repository, self.definition["transforms"])

            # Temporary directory
            tmpdir = tempfile.mkdtemp()
            tmpfile = "%s/file.dl" % tmpdir
            urllib.request.urlretrieve(self.url, tmpfile)
            d = "%s/all" % tmpdir
            tarfile.open(tmpfile).extractall(path="%s/all" % tmpdir)
            outfilename = "%s/qrels" % tmpdir
            
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
            logging.info("Created file %s" % destination)
        finally:
            if not tmpdir is None:
                rm_rf(tmpdir)

