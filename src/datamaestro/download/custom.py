"""Custom download resources.

Provides a Resource subclass that delegates to a user-defined
download function.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from datamaestro import Context
from datamaestro.download import Resource


class Downloader(Protocol):
    def __call__(self, context: Context, root: Path, *, force: bool = False):
        pass


class custom_download(Resource):
    """A resource that delegates to a user-defined download function.

    Usage as class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = custom_download(
                "data", downloader=my_download_fn
            )

    Usage as decorator (deprecated)::

        @custom_download("data", downloader=my_download_fn)
        @dataset(Base)
        def my_dataset(data): ...
    """

    def __init__(
        self,
        varname: str,
        downloader: Downloader,
        *,
        transient: bool = False,
    ):
        super().__init__(varname=varname, transient=transient)
        self.downloader = downloader

    def prepare(self):
        return self.dataset.datapath

    def download(self, force=False):
        self.downloader(self.context, self.dataset.datapath, force=force)
