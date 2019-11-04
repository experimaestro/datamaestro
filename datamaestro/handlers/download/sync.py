import logging
from pathlib import Path

from datamaestro import Dataset
from datamaestro.handlers.download import DownloadHandler

from subprocess import run

class GSync(DownloadHandler):
    """Google sync call"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]

    def download(self, destination: Path):
        if destination.exists(): 
            return
        
        syncpath = destination.parent / ("@%s" % destination.name)
        syncpath.mkdir(exist_ok=True, parents=True)
        logging.info("Synchronizing %s into %s", self.url, syncpath)
        run(["gsutil", "-m", "rsync", self.url, syncpath])

