from typing import Protocol
from pathlib import Path
from datamaestro import Context
from datamaestro.definitions import DatasetWrapper
from datamaestro.download import Resource


class Downloader(Protocol):
    def __call__(self, context: Context, root: Path, *, force=False):
        pass


class CustomResource(Resource):
    def __init__(self, ds_wrapper: DatasetWrapper, downloader: Downloader):
        self.ds_wrapper = ds_wrapper
        self.downloader = downloader

    def prepare(self):
        pass

    def download(self, force=False):
        self.downloader(self.context, self.ds_wrapper.datapath, force=force)


def custom_download(downloader: Downloader) -> Path:
    ds_wrapper = DatasetWrapper.BUILDING[-1]
    ds_wrapper.ordered_resources.append(CustomResource(ds_wrapper, downloader))

    return ds_wrapper.datapath
