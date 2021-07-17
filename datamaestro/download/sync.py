import logging
from pathlib import Path

from datamaestro.download import Download
from datamaestro.definitions import AbstractDataset

from subprocess import run


class gsync(Download):
    """Google sync call"""

    def __init__(self, varname: str, url: str):
        """Synchronize with Google sync

        Args:
            varname: Variable name
            url: The google sync URL (`gs://...`)
        """
        super().__init__(varname)
        self.url = self.definition["url"]

    def download(self, destination: Path):
        if destination.exists():
            return

        syncpath = destination.parent / ("@%s" % destination.name)
        syncpath.mkdir(exist_ok=True, parents=True)
        logging.info("Synchronizing %s into %s", self.url, syncpath)
        run(["gsutil", "-m", "rsync", self.url, syncpath])
