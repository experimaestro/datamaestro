"""Registration of downloaded datasets with ds-cache-cleaner.

Downloaded datasets are recorded in the `ds-cache-cleaner
<https://pypi.org/project/ds-cache-cleaner/>`_ metadata of the data folder
(``<data>/ds-cache-cleaner/``), so that the cleaner (and any other tool
reading it) shows dataset ids, descriptions, sizes and last access times
instead of guessing them from the directory layout.

Once the metadata holds at least one entry, ds-cache-cleaner lists *only*
the registered entries. So that datasets downloaded before the registration
existed do not disappear from it, the first registration in a data folder
runs :func:`sync` (under a lock, once: a marker file records that it was
done). ``datamaestro cache sync`` runs it again, e.g. after datasets were
downloaded by a datamaestro version without registration.

Registration never makes a download fail: errors are logged and ignored.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from ds_cache_cleaner import CacheRegistry, EntryMetadata, MetadataManager
from ds_cache_cleaner.metadata import METADATA_DIR
from filelock import FileLock

if TYPE_CHECKING:
    from datamaestro.context import Context
    from datamaestro.definitions import AbstractDataset

LIBRARY = "datamaestro"
PART = "datasets"

SYNC_MARKER = "datamaestro-synced"
"""File (in the metadata folder) recording that the first sync was done"""

SYNC_LOCK = "datamaestro-sync.lock"


def registry(context: "Context", *, create: bool = True) -> CacheRegistry:
    """Returns the ds-cache-cleaner registry of the context data folder

    :param create: Whether to create the registry metadata if missing
    """
    registry = CacheRegistry(
        context.datapath, library=LIBRARY, description="datamaestro datasets"
    )
    if create:
        registry.register_part(PART, "Downloaded datasets")
    return registry


def entry_path(dataset: "AbstractDataset") -> Optional[str]:
    """Path of the dataset relative to the data folder, or None if the
    dataset is stored elsewhere"""
    try:
        return dataset.datapath.relative_to(dataset.context.datapath).as_posix()
    except ValueError:
        return None


def _metadata_dir(context: "Context") -> Path:
    return context.datapath / METADATA_DIR


def _has_files(dataset: "AbstractDataset") -> bool:
    return any(resource.has_files() for resource in dataset.resources.values())


def _description(dataset: "AbstractDataset") -> str:
    name = getattr(dataset, "name", None)
    return f"{dataset.id}: {name}" if name else str(dataset.id)


def _metadata(dataset: "AbstractDataset") -> dict:
    metadata = {"id": dataset.id}
    if dataset.repository is not None:
        metadata["repository"] = dataset.repository.id
    for key in ("tags", "tasks"):
        if values := getattr(dataset, key, None):
            metadata[key] = sorted(values)
    if dataset.url:
        metadata["url"] = dataset.url
    return metadata


def register(dataset: "AbstractDataset", *, updated: bool = True) -> None:
    """Records that a dataset is on disk

    The first registration in a data folder also registers the datasets
    that were downloaded before (see :func:`sync`).

    :param updated: Whether the dataset files changed; if not and the dataset
        is already registered, only its last access time is updated (which
        avoids recomputing its size)
    """
    try:
        if entry_path(dataset) is None or not _has_files(dataset):
            return
        if not (_metadata_dir(dataset.context) / SYNC_MARKER).exists():
            sync(dataset.context, once=True)
        _register(dataset, updated=updated)
    except Exception:
        logging.warning(
            "Could not register dataset %s with ds-cache-cleaner",
            getattr(dataset, "id", dataset),
            exc_info=True,
        )


def _register(dataset: "AbstractDataset", *, updated: bool = True) -> None:
    path = entry_path(dataset)
    cache = registry(dataset.context)
    if not updated and cache.get_entry(PART, path) is not None:
        cache.touch(PART, path)
        return

    # Size is computed from the filesystem
    cache.register_entry(
        PART, path, description=_description(dataset), metadata=_metadata(dataset)
    )


@dataclass
class SyncReport:
    """What :func:`sync` changed"""

    added: list[str] = field(default_factory=list)
    """Paths of the datasets that were registered"""

    removed: list[str] = field(default_factory=list)
    """Paths of the registered entries whose folder no longer exists"""


def sync(
    context: "Context", *, dry_run: bool = False, once: bool = False
) -> SyncReport:
    """Synchronizes the registry with the data folder

    Registers the datasets that are on disk but not yet registered, and
    removes the entries whose folder has been deleted. This goes through
    all the datasets of all the repositories, and can take a while.

    :param dry_run: Only reports what would change (writes nothing)
    :param once: Does nothing if a sync was already done in this data folder
    """
    if dry_run:
        return _sync(context, dry_run=True)

    metadir = _metadata_dir(context)
    metadir.mkdir(parents=True, exist_ok=True)
    marker = metadir / SYNC_MARKER

    # Concurrent processes wait for the sync rather than running their own
    with FileLock(metadir / SYNC_LOCK):
        if once and marker.exists():
            return SyncReport()
        if once:
            logging.info(
                "Registering the downloaded datasets with ds-cache-cleaner "
                "(done once, can take a while)"
            )
        report = _sync(context, dry_run=False)
        marker.touch()
    return report


def _sync(context: "Context", *, dry_run: bool) -> SyncReport:
    report = SyncReport()
    cache = registry(context, create=not dry_run)
    # Reading takes a lock, which would create the metadata folder
    exists = MetadataManager(context.datapath).exists
    registered = (
        {entry.path: entry for entry in cache.list_entries(PART)} if exists else {}
    )

    for path in registered:
        if not (context.datapath / path).exists():
            report.removed.append(path)
            if not dry_run:
                cache.remove(PART, path)

    for dataset in context.datasets():
        try:
            path = entry_path(dataset)
            if path is None or path in registered or not _has_files(dataset):
                continue
            if not dataset.is_prepared():
                continue
            if not dry_run:
                _register(dataset)
        except Exception:
            logging.warning("Could not register dataset %s", dataset, exc_info=True)
            continue

        report.added.append(path)
        registered[path] = EntryMetadata(path=path)

    return report
