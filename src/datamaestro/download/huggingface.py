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
import re
from pathlib import Path

from datamaestro.download import (
    CheckStatus,
    FolderResource,
    ResourceCheckResult,
    ValueResource,
)

logger = logging.getLogger(__name__)


# ---- Shared "materialise on disk" helper ---------------------------------

# A split expression we can map back to a single split: a bare name, or a
# name followed by a slice (``train[:10%]``). Compound expressions such as
# ``train+test`` deliberately do not match.
_SPLIT_RE = re.compile(r"^([\w.\-]+)(\[.*\])?$")


def _base_split(split: str | None) -> str | None:
    """The split name ``split`` refers to, or None if it spans several."""
    if split is None:
        return None
    m = _SPLIT_RE.match(split.strip())
    return m.group(1) if m else None


def _find_cached_hashed_builder(source: str, name: str):
    """Attempt to find a cached builder matching a hashed config name (e.g. ``quora-ccbd7fec3e15cba7``).

    This happens when ``hf_builder`` restricted ``data_files`` during initial preparation,
    which caused HF ``datasets`` to save the dataset under a hashed config name.
    In offline mode, a plain ``load_dataset_builder(source, name)`` fails because the un-hashed
    config directory does not exist on disk.

    Note on HF ``datasets`` caching quirk:
    HF ``datasets``' ``cache._find_hash_in_cache`` requires ``dataset_info.json``'s ``config_name``
    field to match the cache subfolder name (``parts[-3]``). When a hashed builder is prepared,
    HF ``datasets`` writes the unhashed ``config_name`` into ``dataset_info.json``. To allow
    HF ``datasets`` cache loader to load the hashed config offline, we ensure ``dataset_info.json``
    contains the matching hashed config name.
    """
    import json

    try:
        from datasets import load_dataset_builder
        from datasets.config import HF_DATASETS_CACHE
    except ModuleNotFoundError:
        return None

    repo_dir = Path(HF_DATASETS_CACHE) / source.replace("/", "___")
    if not repo_dir.exists():
        repo_dir_alt = Path(HF_DATASETS_CACHE) / source.replace("/", "---")
        if repo_dir_alt.exists():
            repo_dir = repo_dir_alt
        else:
            return None

    matches = sorted(
        [d for d in repo_dir.iterdir() if d.is_dir() and d.name.startswith(f"{name}-")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not matches:
        return None

    target_dir = matches[0]
    hashed_name = target_dir.name

    # Check and patch dataset_info.json if HF datasets wrote un-hashed config_name
    for info_file in target_dir.glob("*/*/dataset_info.json"):
        try:
            info = json.loads(info_file.read_text(encoding="utf-8"))
            if info.get("config_name") != hashed_name:
                logger.info(
                    "[hf] Updating %s config_name from '%s' to '%s' for HF datasets cache compatibility",
                    info_file,
                    info.get("config_name"),
                    hashed_name,
                )
                info["config_name"] = hashed_name
                info_file.write_text(json.dumps(info, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning(
                "[hf] Could not update dataset_info.json at %s: %s", info_file, exc
            )

    logger.info(
        "[hf] Offline fallback: using cached split-restricted config '%s' for '%s' (requested config '%s')",
        hashed_name,
        source,
        name,
    )
    return load_dataset_builder(source, hashed_name)


def _find_cached_hashed_builder(source: str, name: str):
    """Attempt to find a cached builder matching a hashed config name (e.g. ``quora-ccbd7fec3e15cba7``).

    This happens when ``hf_builder`` restricted ``data_files`` during initial preparation,
    which caused HF ``datasets`` to save the dataset under a hashed config name.
    In offline mode, a plain ``load_dataset_builder(source, name)`` fails because the un-hashed
    config directory does not exist on disk.

    Note on HF ``datasets`` caching quirk:
    HF ``datasets``' ``cache._find_hash_in_cache`` requires ``dataset_info.json``'s ``config_name``
    field to match the cache subfolder name (``parts[-3]``). When a hashed builder is prepared,
    HF ``datasets`` writes the unhashed ``config_name`` into ``dataset_info.json``. To allow
    HF ``datasets`` cache loader to load the hashed config offline, we ensure ``dataset_info.json``
    contains the matching hashed config name.
    """
    import json

    try:
        from datasets import load_dataset_builder
        from datasets.config import HF_DATASETS_CACHE
    except ModuleNotFoundError:
        return None

    repo_dir = Path(HF_DATASETS_CACHE) / source.replace("/", "___")
    if not repo_dir.exists():
        repo_dir_alt = Path(HF_DATASETS_CACHE) / source.replace("/", "---")
        if repo_dir_alt.exists():
            repo_dir = repo_dir_alt
        else:
            return None

    matches = sorted(
        [d for d in repo_dir.iterdir() if d.is_dir() and d.name.startswith(f"{name}-")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not matches:
        return None

    target_dir = matches[0]
    hashed_name = target_dir.name

    # Check and patch dataset_info.json if HF datasets wrote un-hashed config_name
    for info_file in target_dir.glob("*/*/dataset_info.json"):
        try:
            info = json.loads(info_file.read_text(encoding="utf-8"))
            if info.get("config_name") != hashed_name:
                logger.info(
                    "[hf] Updating %s config_name from '%s' to '%s' for HF datasets cache compatibility",
                    info_file,
                    info.get("config_name"),
                    hashed_name,
                )
                info["config_name"] = hashed_name
                info_file.write_text(json.dumps(info, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning(
                "[hf] Could not update dataset_info.json at %s: %s", info_file, exc
            )

    logger.info(
        "[hf] Offline fallback: using cached split-restricted config '%s' for '%s' (requested config '%s')",
        hashed_name,
        source,
        name,
    )
    return load_dataset_builder(source, hashed_name)


def hf_builder(
    source: str,
    name: str | None = None,
    data_files: str | None = None,
    split: str | None = None,
    revision: str | None = None,
):
    """Build a ``datasets`` builder, restricted to ``split`` when possible.

    ``DatasetBuilder.download_and_prepare`` has no split argument: it always
    prepares every split the builder knows about. The one lever available is
    the set of data files, so when ``split`` names a single split *and* the
    builder resolved its data files per split (the case for all packaged
    builders — parquet, json, csv, …), we rebuild it over that split's files
    only. Script-based builders, which expose no per-split ``data_files``,
    fall back to preparing everything.

    Returns:
        A ``(builder, restricted)`` pair, ``restricted`` telling whether the
        builder covers only ``split``.
    """
    try:
        from datasets import load_dataset_builder
    except ModuleNotFoundError:
        logger.error("the datasets library is not installed:")
        logger.error("pip install datasets")
        raise

    try:
        builder = load_dataset_builder(
            source, name, data_files=data_files, revision=revision
        )
    except Exception as exc:
        if name is not None and data_files is None:
            logger.warning(
                "[hf] load_dataset_builder failed for source='%s', name='%s' (%s). "
                "Checking for cached split-restricted hashed configs in HF cache...",
                source,
                name,
                exc,
            )
            cached_builder = _find_cached_hashed_builder(source, name)
            if cached_builder is not None:
                return cached_builder, True
        raise

    base = _base_split(split)
    if base is None:
        return builder, False

    resolved = builder.config.data_files
    # Nothing to gain when the builder already covers a single split.
    if not isinstance(resolved, dict) or base not in resolved or len(resolved) <= 1:
        return builder, False

    logger.debug(
        "[hf] restricting %s to split %r (%d/%d splits)",
        source,
        base,
        1,
        len(resolved),
    )
    restricted = load_dataset_builder(
        source, name, data_files={base: list(resolved[base])}, revision=revision
    )
    return restricted, True


def hf_download_and_prepare(
    source: str,
    name: str | None = None,
    data_files: str | None = None,
    split: str | None = None,
    revision: str | None = None,
):
    """Materialise a HuggingFace dataset in the local cache.

    Goes through ``download_and_prepare`` rather than ``load_dataset``: the
    Arrow shards are written to disk without the ``Dataset`` object ever
    being instantiated, which is what makes this usable on a login node for
    a large dataset. Idempotent — a warm cache makes it a near no-op.

    Returns the builder, so callers that *do* want the data can follow up
    with ``builder.as_dataset(split=...)``. Going through the same builder
    matters: restricting the data files changes the builder's ``config_id``,
    hence its cache directory, so a caller that prepared here and then read
    via a plain ``load_dataset`` would miss the cache and fetch everything
    a second time. The flip side is that asking for one split and later for
    the whole dataset fills two cache directories — a split-restricted build
    is a saving for pipelines that only ever want that split.
    """
    builder, restricted = hf_builder(
        source, name, data_files=data_files, split=split, revision=revision
    )

    # A split-restricted or data_files-restricted build records fewer splits than the dataset
    # metadata declares, which trips ``verify_splits``.
    kwargs = (
        {"verification_mode": "no_checks"}
        if (restricted or data_files is not None or split is not None)
        else {}
    )
    builder.download_and_prepare(**kwargs)
    return builder


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
        revision: str | None = None,
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
            revision: HuggingFace git commit SHA, branch, or tag.
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
        self.revision = revision
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

        # Streaming mode materialises nothing locally: there is no download
        # to perform, and building the iterator here would be wasted work.
        if self.streaming:
            return True

        hf_download_and_prepare(
            self.repo_id,
            self.config_name,
            data_files=self.data_files,
            split=self.split,
            revision=self.revision,
        )
        return True

    def prepare(self):
        return {
            "repo_id": self.repo_id,
            "name": self.config_name,
            "data_files": self.data_files,
            "split": self.split,
            "revision": self.revision,
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
