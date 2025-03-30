from typing import Protocol
from pathlib import Path
from datamaestro import Context
from datamaestro.download import Resource


class Downloader(Protocol):
    def __call__(self, context: Context, root: Path, *, force=False):
        pass


class custom_download(Resource):
    def __init__(self, varname: str, downloader: Downloader):
        super().__init__(varname)
        self.downloader = downloader

    def prepare(self):
        return self.definition.datapath

    def download(self, force=False):
        self.downloader(self.context, self.definition.datapath, force=force)
