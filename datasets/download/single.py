import logging
import shutil
import tempfile
import gzip
import urllib.request
import os.path as op, os

from ..data import DownloadHandler

class File(DownloadHandler):
    """Single file"""
    def __init__(self, definition):
        super().__init__(definition)
        self.url = self.definition["url"]
        self.gzip = self.url.endswith(".gz")

    def download(self, destination):
        logging.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download
        downloadedfile = tempfile.NamedTemporaryFile(delete=True)
        urllib.request.urlretrieve(self.url, downloadedfile.name)

        # Uncompress
        file = downloadedfile
        if self.gzip:
            file = tempfile.NamedTemporaryFile(delete=True)
            f = gzip.open(downloadedfile.name, mode="r")
            file.writelines(f)
            f.close()
            logging.info("Uncompressed in %s" % file.name)

        # Move
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
    def __init__(self, definition):
        super().__init__(definition)
        self.url = self.definition["url"]
        self.gzip = self.url.endswith(".gz")

    def download(self, destination):
        tmpdir = None
        try:
            # Temporary directory
            tmpdir = tempfile.mkdtemp()
            tmpfile = "%s/file.dl" % tmpdir
            urllib.request.urlretrieve(url, tmpfile)
            d = "%s/all" % tmpdir
            tarfile.open(tmpfile).extractall(path="%s/all" % tmpdir)
            f_out = open("%s/qrels" % tmpdir, 'w')
            for aPath in (os.path.join(d, f) for f in os.listdir(d)):
                logging.info("Uncompressing %s" % aPath)
                if args[0]:
                    gzf = gzip.open(aPath, "rt")
                else:
                    gzf = open(aPath, "r")
                f_out.write(gzf.read())
                gzf.close()
            f_out.close()

            # Move in place
            shutil.move("%s/qrels" % tmpdir, path)
            logging.info("Created file %s" % path)
        finally:
            if not tmpdir is None:
                rm_rf(tmpdir)

