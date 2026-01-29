"""HuggingFace Hub download resources.

Provides a ValueResource subclass for loading datasets from
the HuggingFace Hub.
"""

from __future__ import annotations

import logging

from datamaestro.download import ValueResource

logger = logging.getLogger(__name__)


class HFDownloader(ValueResource):
    """Load a dataset from the HuggingFace Hub.

    Usage as class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = HFDownloader.apply(
                "hf_data", repo_id="user/dataset"
            )

    Usage as decorator (deprecated)::

        @hf_download("hf_data", repo_id="user/dataset")
        @dataset(Base)
        def my_dataset(hf_data): ...
    """

    def __init__(
        self,
        varname: str,
        repo_id: str,
        *,
        data_files: str | None = None,
        split: str | None = None,
        transient: bool = False,
    ):
        """
        Args:
            varname: Variable name.
            repo_id: The HuggingFace repository ID.
            data_files: Specific data files to load.
            split: Dataset split to load.
            transient: If True, data can be deleted after dependents
                complete.
        """
        super().__init__(varname=varname, transient=transient)
        self.repo_id = repo_id
        self.data_files = data_files
        self.split = split

    def download(self, force=False):
        try:
            from datasets import load_dataset
        except ModuleNotFoundError:
            logger.error("the datasets library is not installed:")
            logger.error("pip install datasets")
            raise

        self._dataset = load_dataset(self.repo_id, data_files=self.data_files)
        return True

    def prepare(self):
        return {
            "repo_id": self.repo_id,
            "data_files": self.data_files,
            "split": self.split,
        }


# Factory alias for backward compat
hf_download = HFDownloader.apply
