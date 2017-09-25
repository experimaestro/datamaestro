import logging
import shutil
import tempfile
import gzip
import urllib.request
import os.path as op, os

from ..data import DownloadHandler

class Gz(DownloadHandler):
    def __init__(self, definition):
        super().__init__(definition)
        self.url = self.definition["url"]

    def download(self, destination):
        """Get a GZIPPED file over the network and uncompress it"""
        logging.info("Downloading %s into %s", self.url, destination)

        # Creates directory if needed
        dir = op.dirname(destination)
        os.makedirs(dir, exist_ok=True)

        # Download
        gzfile = tempfile.NamedTemporaryFile(delete=True)
        urllib.request.urlretrieve(self.url, gzfile.name)

        # Uncompress
        file = tempfile.NamedTemporaryFile(delete=True)
        f = gzip.open(gzfile.name, mode="r")
        file.writelines(f)
        f.close()

        # Move
        logging.info("Uncompressed in %s" % file.name)
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
