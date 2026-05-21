"""HuggingFace Hub download resources.

Provides two kinds of resources:

* :class:`HFDownloader` — a :class:`ValueResource` wrapping
  ``datasets.load_dataset`` for repos in HF "Datasets"-format.
* :class:`HFSnapshotDownloader` — a :class:`FolderResource` wrapping
  ``huggingface_hub.snapshot_download`` for repos where we want to
  materialise a selected subset of raw files on disk (e.g. pre-tokenised
  shards, model checkpoints, anything ``load_dataset`` cannot parse).

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
    FolderResource,
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

        # Consult ``hf_resolver`` helpers (e.g. a cluster mirror plugin)
        # before reaching the network. The first hit wins; if none match,
        # we fall through to the normal ``load_dataset`` path below.
        from datamaestro.helpers import get_helpers

        for resolver in get_helpers("hf_resolver"):
            try:
                p = resolver.find_dataset(
                    self.repo_id, self.config_name, self.data_files
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "hf_resolver %s.find_dataset raised; skipping",
                    type(resolver).__name__,
                )
                continue
            if p is not None:
                self.local_path = Path(p)
                logger.info(
                    "[HFDownloader] %s served from local mirror by %s (no network): %s",
                    self.repo_id,
                    type(resolver).__name__,
                    self.local_path,
                )
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


class HFSnapshotDownloader(FolderResource):
    """Download a selected pattern of files from an HF Hub repo on disk.

    Unlike :class:`HFDownloader` (which goes through ``load_dataset``), this
    resource wraps :func:`huggingface_hub.snapshot_download` and materialises
    the matching files into a local directory. Use it for repos containing
    raw shards or other files that the ``datasets`` library cannot parse.

    Usage as class attribute (preferred)::

        @dataset(MyType, url="...")
        class MyDataset(Base):
            SHARDS = HFSnapshotDownloader.apply(
                "shards",
                repo_id="org/repo",
                repo_type="dataset",
                allow_patterns=["folder/*.jsonl.tar.gz"],
            )
    """

    def __init__(
        self,
        varname: str,
        repo_id: str,
        *,
        repo_type: str = "dataset",
        allow_patterns: list[str] | None = None,
        ignore_patterns: list[str] | None = None,
        revision: str | None = None,
        transient: bool = False,
    ):
        super().__init__(varname=varname, transient=transient)
        self.repo_id = repo_id
        self.repo_type = repo_type
        self.allow_patterns = allow_patterns
        self.ignore_patterns = ignore_patterns
        self.revision = revision

    @property
    def path(self) -> Path:
        # When a registered ``hf_resolver`` plugin can serve this repo
        # from a local mirror (e.g. ``$DSDIR/HuggingFace_Models/<org>/<name>``
        # on an HPC cluster), expose that directory as the resource path.
        # The framework's "files present?" check then passes without us
        # ever downloading or symlinking.
        if (p := self._resolved_path()) is not None:
            return p
        return super().path

    def _resolved_path(self) -> Path | None:
        from datamaestro.helpers import get_helpers

        for resolver in get_helpers("hf_resolver"):
            try:
                p = resolver.find_model(self.repo_id, self.revision)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "hf_resolver %s.find_model raised; skipping",
                    type(resolver).__name__,
                )
                continue
            if p is not None:
                return Path(p)
        return None

    def _download(self, destination: Path) -> None:
        # If a resolver served the repo, nothing to do — ``path`` already
        # points at the mirror.
        if (p := self._resolved_path()) is not None:
            logger.info(
                "[HFSnapshotDownloader] %s served from local mirror (no network): %s",
                self.repo_id,
                p,
            )
            return

        try:
            from huggingface_hub import snapshot_download
        except ModuleNotFoundError:
            logger.error("the huggingface_hub library is not installed")
            raise

        destination.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Snapshot-downloading %s (type=%s, patterns=%s) into %s",
            self.repo_id,
            self.repo_type,
            self.allow_patterns,
            destination,
        )
        snapshot_download(
            repo_id=self.repo_id,
            repo_type=self.repo_type,
            allow_patterns=self.allow_patterns,
            ignore_patterns=self.ignore_patterns,
            revision=self.revision,
            local_dir=str(destination),
        )

    def check(self) -> ResourceCheckResult:
        import requests

        url = f"https://huggingface.co/api/{self.repo_type}s/{self.repo_id}"
        try:
            response = requests.head(url, allow_redirects=True, timeout=30)
            ok = response.status_code < 400
            return ResourceCheckResult(
                resource=self.name,
                status=CheckStatus.OK if ok else CheckStatus.FAILED,
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
