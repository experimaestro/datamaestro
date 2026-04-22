"""HuggingFace Hub download resources.

Provides a ValueResource subclass for loading datasets from the
HuggingFace Hub.

Convention: parameters that change *which* dataset is loaded (``repo_id``,
``name``, ``data_files``, ``split``) contribute to the dataset identity;
parameters that only change *how* it is loaded (``streaming``,
``local_path``) do not.
"""

from __future__ import annotations

import logging
from pathlib import Path

from datamaestro.download import (
    CheckStatus,
    ResourceCheckResult,
    ValueResource,
)

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
        name: str | None = None,
        data_files: str | None = None,
        split: str | None = None,
        streaming: bool = False,
        local_path: Path | str | None = None,
        transient: bool = False,
    ):
        """
        Args:
            varname: Variable name.
            repo_id: The HuggingFace repository ID.
            name: The HF dataset config name (the second positional
                argument to ``datasets.load_dataset``).
            data_files: Specific data files to load.
            split: Dataset split to load.
            streaming: If True, iterate the dataset in streaming mode
                without materialising to local disk.
            local_path: If set, load from this local mirror instead of
                the HuggingFace Hub.
            transient: If True, data can be deleted after dependents
                complete.
        """
        super().__init__(varname=varname, transient=transient)
        self.repo_id = repo_id
        # Stored as `config_name` to avoid shadowing `Resource.name`
        # (which holds the resource varname). The HF API calls this `name`.
        self.config_name = name
        self.data_files = data_files
        self.split = split
        self.streaming = streaming
        self.local_path = Path(local_path) if local_path is not None else None

    def download(self, force=False):
        # When loading from a local mirror, there is nothing to download.
        if self.local_path is not None:
            return True

        try:
            from datasets import load_dataset
        except ModuleNotFoundError:
            logger.error("the datasets library is not installed:")
            logger.error("pip install datasets")
            raise

        self._dataset = load_dataset(
            self.repo_id,
            self.config_name,
            data_files=self.data_files,
            split=self.split,
            streaming=self.streaming,
        )
        return True

    def prepare(self):
        return {
            "repo_id": self.repo_id,
            "name": self.config_name,
            "data_files": self.data_files,
            "split": self.split,
            "streaming": self.streaming,
            "local_path": str(self.local_path) if self.local_path else None,
        }

    def check(self):
        if self.local_path is not None:
            exists = self.local_path.exists()
            return ResourceCheckResult(
                resource=self.name,
                status=CheckStatus.OK if exists else CheckStatus.FAILED,
                message=(
                    "local mirror present"
                    if exists
                    else f"local mirror missing: {self.local_path}"
                ),
                url=str(self.local_path),
            )

        import requests

        url = f"https://huggingface.co/api/datasets/{self.repo_id}"
        try:
            response = requests.head(url, allow_redirects=True, timeout=30)
            if response.status_code < 400:
                return ResourceCheckResult(
                    resource=self.name,
                    status=CheckStatus.OK,
                    message=f"HTTP {response.status_code}",
                    url=url,
                )
            else:
                return ResourceCheckResult(
                    resource=self.name,
                    status=CheckStatus.FAILED,
                    message=f"HTTP {response.status_code}",
                    url=url,
                )
        except Exception as e:
            return ResourceCheckResult(
                resource=self.name,
                status=CheckStatus.ERROR,
                message=str(e),
                url=url,
            )


# Factory alias for backward compat
hf_download = HFDownloader.apply
