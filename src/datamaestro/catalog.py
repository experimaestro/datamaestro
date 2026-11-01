"""Catalog of the available datasets

Listing the datasets means importing all the dataset definition modules of
all the repositories, which is slow. The catalog stores what is needed to
browse and search the datasets (ids, names, descriptions, tags, types,
variants...) as plain data, and caches it as JSON in the datamaestro cache
folder. The cache is rebuilt when a repository changes (version or
modification time of its definition files).

The datasets depend on the Python environment (installed repositories and
their versions): with uv, each project has its own environment. The cache
is thus stored in the environment itself (``<sys.prefix>/share/datamaestro``).
When the environment is not writable, or is temporary (e.g. ``uv run
--with ...``), it is stored in the datamaestro cache folder
(``cache/catalog/``), under a name derived from the installed repositories
and their versions; only the most recently used files are kept.

Downloaded datasets that are not defined in the current environment can
be listed from the ds-cache-cleaner registry (see
:meth:`Catalog.with_downloads`).

A dataset *family* with variants has one entry; its variant axes are
described by :class:`VariantsInfo`. Use :meth:`DatasetEntry.dataset` to load
the dataset itself (e.g. to resolve a variant selector).
"""

import hashlib
import itertools
import json
import os
import sys
import logging
import re
import traceback
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Union,
)

if TYPE_CHECKING:
    from ds_cache_cleaner import EntryMetadata

    from datamaestro.context import BaseRepository, Context
    from datamaestro.definitions import AbstractDataset
    from datamaestro.variants import Variants

CATALOG_VERSION = 2
"""Version of the cache format"""

CACHE_NAME = "catalog.json"

MAX_SHARED_CACHES = 10
"""Number of catalogs kept in the datamaestro cache folder"""

MAX_EXAMPLES = 20
"""Maximum number of variants listed for non-axes variant spaces"""


@dataclass
class VariantAxis:
    """One axis of a variant space

    Values are stored in their selector form (e.g. ``true``, ``null``)
    """

    name: str
    type: Optional[str] = None
    domain: Optional[List[str]] = None
    """Possible values, or None if the axis is open"""

    default: Optional[str] = None
    required: bool = False
    description: str = ""
    in_id: bool = True
    elide_default: bool = False


@dataclass
class VariantsInfo:
    """Describes the variants of a dataset family"""

    axes: Optional[List[VariantAxis]] = None
    """The axes (None if the variant space is not made of axes)"""

    examples: List[Dict[str, str]] = field(default_factory=list)
    """Some variants (when the variant space is not made of axes)"""

    documentation: str = ""
    """reStructuredText documentation of the variant space"""

    @staticmethod
    def from_variants(variants: "Variants") -> "VariantsInfo":
        from datamaestro.variants import AxesVariants, _axis_attr_docs, _pretty_type

        try:
            documentation = variants.document()
        except Exception:
            documentation = ""

        if not isinstance(variants, AxesVariants):
            examples = [
                {key: _format_value(value) for key, value in combo.items()}
                for combo in itertools.islice(variants.enumerate(), MAX_EXAMPLES)
            ]
            return VariantsInfo(examples=examples, documentation=documentation)

        docs = _axis_attr_docs(type(variants))
        axes = [
            VariantAxis(
                name=name,
                type=_pretty_type(axis.type) or None,
                domain=(
                    None
                    if axis.domain is None
                    else [_format_value(value) for value in axis.domain]
                ),
                default=_format_value(axis.default) if axis.has_default else None,
                required=not axis.has_default,
                description=docs.get(name) or axis.description,
                in_id=axis.in_id,
                elide_default=axis.elide_default,
            )
            for name, axis in variants.axes.items()
        ]
        return VariantsInfo(axes=axes, documentation=documentation)

    @staticmethod
    def from_json(data: dict) -> "VariantsInfo":
        axes = data.get("axes")
        return VariantsInfo(
            axes=None if axes is None else [VariantAxis(**axis) for axis in axes],
            examples=data.get("examples", []),
            documentation=data.get("documentation", ""),
        )

    def values(self) -> Iterator[str]:
        """All the known variant values (used for searching)"""
        for axis in self.axes or []:
            yield from axis.domain or []
            if axis.default is not None:
                yield axis.default
        for example in self.examples:
            yield from example.values()


def _format_value(value) -> str:
    from datamaestro.variants import AxesVariants

    return AxesVariants._format_value(value)


@dataclass
class DatasetEntry:
    """What the catalog knows about a dataset (no import needed)"""

    id: str
    name: str = ""
    description: str = ""
    aliases: List[str] = field(default_factory=list)
    repository: Optional[str] = None
    module: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    tasks: List[str] = field(default_factory=list)
    config_type: Optional[str] = None
    """Qualified name of the configuration type"""

    types: List[str] = field(default_factory=list)
    """Experimaestro identifiers of the configuration type and its ancestors"""

    url: Optional[str] = None
    doi: Optional[str] = None
    path: Optional[str] = None
    """Data folder of the dataset, relative to the datamaestro data folder"""

    requirements: List[str] = field(default_factory=list)
    variants: Optional[VariantsInfo] = None

    repository_version: Optional[str] = None
    """Version of the package providing the repository"""

    available: bool = True
    """False if the dataset is not defined in the current environment (it
    is only known from the registry of downloaded datasets)"""

    @staticmethod
    def from_dataset(dataset: "AbstractDataset") -> "DatasetEntry":
        from datamaestro import cleaner

        try:
            description = dataset.description or ""
        except NotImplementedError:
            description = ""

        configtype = getattr(dataset, "base", None)
        try:
            types = [
                str(ancestor.__getxpmtype__().identifier)
                for ancestor in dataset.ancestors()
            ]
        except Exception:
            types = []

        variants = getattr(dataset, "variants", None)
        definition = getattr(dataset, "t", type(dataset))
        try:
            path = cleaner.entry_path(dataset)
        except Exception:
            path = None

        return DatasetEntry(
            id=dataset.id,
            name=dataset.name or "",
            description=description,
            aliases=sorted(set(dataset.aliases) - {dataset.id}),
            repository=dataset.repository.id if dataset.repository else None,
            repository_version=(
                cleaner.repository_version(dataset.repository)
                if dataset.repository
                else None
            ),
            module=definition.__module__,
            tags=sorted(dataset.tags),
            tasks=sorted(dataset.tasks),
            config_type=(
                f"{configtype.__module__}.{configtype.__qualname__}"
                if configtype is not None
                else None
            ),
            types=types,
            url=dataset.url,
            doi=dataset.doi,
            path=path,
            requirements=[str(r) for r in dataset.requirements],
            variants=(
                None if variants is None else VariantsInfo.from_variants(variants)
            ),
        )

    @staticmethod
    def from_registry(registered: "EntryMetadata") -> "DatasetEntry":
        """Entry of a downloaded dataset, from its registry metadata"""
        metadata = registered.metadata
        dataset_id = metadata.get("id") or registered.path
        name = registered.description
        if name.startswith(f"{dataset_id}: "):
            name = name[len(dataset_id) + 2 :]
        elif name == dataset_id:
            name = ""
        return DatasetEntry(
            id=dataset_id,
            name=name,
            repository=metadata.get("repository"),
            repository_version=metadata.get("repository_version"),
            module=metadata.get("module"),
            tags=list(metadata.get("tags", [])),
            tasks=list(metadata.get("tasks", [])),
            url=metadata.get("url"),
            path=registered.path,
            available=False,
        )

    def to_json(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_json(data: dict) -> "DatasetEntry":
        data = dict(data)
        if (variants := data.get("variants")) is not None:
            data["variants"] = VariantsInfo.from_json(variants)
        return DatasetEntry(**data)

    def dataset(self, context: Optional["Context"] = None) -> "AbstractDataset":
        """Loads the dataset (imports its definition module)"""
        import importlib

        from datamaestro.context import Context, Datasets

        if self.module:
            try:
                for dataset in Datasets(importlib.import_module(self.module)):
                    if dataset.id == self.id:
                        return dataset
            except Exception:
                logging.debug("Could not load %s from %s", self.id, self.module)

        return (context or Context.instance()).dataset(self.id)

    def search_texts(self) -> Iterator[str]:
        """Texts matched by free search terms"""
        yield self.id
        yield from self.aliases
        yield self.name
        yield self.description
        yield from self.tags
        yield from self.tasks
        if self.variants is not None:
            yield from self.variants.values()


@dataclass
class LoadError:
    """A dataset module (or repository) that could not be loaded"""

    source: str
    error: str
    details: str = ""


# --- Search


def _regex(pattern: str) -> re.Pattern:
    try:
        return re.compile(pattern, re.IGNORECASE)
    except re.error:
        return re.compile(re.escape(pattern), re.IGNORECASE)


_FIELDS = {
    "tag": lambda entry: entry.tags,
    "task": lambda entry: entry.tasks,
    "type": lambda entry: entry.types,
    "repo": lambda entry: [entry.repository or ""],
    "repository": lambda entry: [entry.repository or ""],
    "id": lambda entry: [entry.id, *entry.aliases],
}


def parse_query(query: Union[str, Sequence[str]]):
    """Parses a query into a list of (field or None, regex) conditions

    The query is a list of terms (all must match). A term ``field:REGEX``
    restricts the search to a field (``tag``, ``task``, ``type``, ``repo``,
    ``id``); other terms are matched against the id, name, description,
    tags, tasks and variant values. The matching is case-insensitive.
    """
    terms = query.split() if isinstance(query, str) else query
    conditions = []
    for term in terms:
        prefix, sep, rest = term.partition(":")
        if sep and prefix in _FIELDS:
            conditions.append((prefix, _regex(rest)))
        else:
            conditions.append((None, _regex(term)))
    return conditions


def _matches(entry: DatasetEntry, conditions) -> bool:
    for name, regex in conditions:
        texts = entry.search_texts() if name is None else _FIELDS[name](entry)
        if not any(regex.search(text) for text in texts if text):
            return False
    return True


# --- Catalog


class Catalog:
    """The available datasets"""

    def __init__(
        self, entries: Iterable[DatasetEntry], errors: Iterable[LoadError] = ()
    ):
        self.entries: Dict[str, DatasetEntry] = {}
        for entry in entries:
            self.entries.setdefault(entry.id, entry)
        self.errors = list(errors)

        self._aliases: Dict[str, DatasetEntry] = {}
        for entry in self.entries.values():
            for alias in entry.aliases:
                self._aliases.setdefault(alias, entry)

    def __len__(self):
        return len(self.entries)

    def __iter__(self) -> Iterator[DatasetEntry]:
        return iter(self.entries.values())

    def get(self, dataset_id: str) -> Optional[DatasetEntry]:
        """Returns the entry of a dataset (a variant selector is ignored)"""
        from datamaestro.variants import split_id_selector

        base_id, _ = split_id_selector(dataset_id)
        return self.entries.get(base_id) or self._aliases.get(base_id)

    def search(self, query: Union[str, Sequence[str]] = ()) -> List[DatasetEntry]:
        """Returns the datasets matching the query (see :func:`parse_query`)"""
        conditions = parse_query(query)
        return [entry for entry in self if _matches(entry, conditions)]

    def facets(
        self, entries: Optional[Iterable[DatasetEntry]] = None
    ) -> Dict[str, Counter]:
        """Counts the tags, tasks and repositories of the entries"""
        facets = {"repository": Counter(), "tag": Counter(), "task": Counter()}
        for entry in self if entries is None else entries:
            if entry.repository:
                facets["repository"][entry.repository] += 1
            facets["tag"].update(entry.tags)
            facets["task"].update(entry.tasks)
        return facets

    def with_downloads(self, downloads: Dict[str, "EntryMetadata"]) -> "Catalog":
        """Adds the downloaded datasets that are not defined in the current
        environment (e.g. defined by another version of their repository)

        :param downloads: The registered datasets (see
            :func:`datamaestro.cleaner.entries`)
        """
        paths = {entry.path for entry in self if entry.path}
        missing = [
            DatasetEntry.from_registry(registered)
            for path, registered in sorted(downloads.items())
            if path not in paths
        ]
        return Catalog([*self, *missing], self.errors)

    # --- Building

    @staticmethod
    def build(context: "Context") -> "Catalog":
        """Builds the catalog by loading all the datasets"""
        entries: List[DatasetEntry] = []
        errors: List[LoadError] = []

        def add(dataset):
            try:
                entries.append(DatasetEntry.from_dataset(dataset))
            except Exception as e:
                errors.append(_error(str(getattr(dataset, "id", dataset)), e))

        for repository in context.repositories():
            for source, datasets in _repository_datasets(repository):
                try:
                    for dataset in datasets:
                        add(dataset)
                except Exception as e:
                    errors.append(_error(source, e))

        return Catalog(entries, errors)

    # --- Cache

    @staticmethod
    def cache_path(context: "Context", fingerprint=None) -> Path:
        """Where the catalog of the current environment is cached

        :param fingerprint: The installed repositories (computed if not given)
        """
        folder = _environment_cache_dir()
        if not _is_temporary(folder):
            try:
                folder.mkdir(parents=True, exist_ok=True)
                if os.access(folder, os.W_OK):
                    return folder / CACHE_NAME
            except OSError:
                pass

        # Shared folder: the name depends on the installed repositories
        if fingerprint is None:
            fingerprint = _fingerprint(context)
        key = hashlib.sha256(
            json.dumps(fingerprint, sort_keys=True).encode()
        ).hexdigest()[:16]
        return context.cachepath / "catalog" / f"{key}.json"

    @staticmethod
    def load(context: "Context", *, refresh: bool = False) -> "Catalog":
        """Returns the catalog, using the cache if it is up to date

        :param refresh: Rebuilds the catalog even if the cache is up to date
        """
        fingerprint = _fingerprint(context)
        path = Catalog.cache_path(context, fingerprint)

        if not refresh:
            try:
                data = json.loads(path.read_text())
                if (
                    data.get("version") == CATALOG_VERSION
                    and data.get("fingerprint") == fingerprint
                ):
                    _used(path)
                    return Catalog.from_json(data)
            except FileNotFoundError:
                pass
            except Exception:
                logging.warning("Could not read the catalog cache %s", path)

        catalog = Catalog.build(context)
        try:
            catalog.save(path, fingerprint)
            _used(path)
        except OSError:
            logging.warning("Could not write the catalog cache %s", path)
        return catalog

    def save(self, path: Path, fingerprint=None):
        data = {
            "version": CATALOG_VERSION,
            "fingerprint": fingerprint,
            "entries": [entry.to_json() for entry in self],
            "errors": [asdict(error) for error in self.errors],
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(path)

    @staticmethod
    def from_json(data: dict) -> "Catalog":
        return Catalog(
            [DatasetEntry.from_json(entry) for entry in data["entries"]],
            [LoadError(**error) for error in data.get("errors", [])],
        )


def _environment_cache_dir() -> Path:
    return Path(sys.prefix) / "share" / "datamaestro"


def _is_temporary(folder: Path) -> bool:
    """Whether the environment is temporary (uv ``--with`` environments live
    in a temporary folder of the uv cache ``builds-v0`` folder)"""
    return any(parent.name == "builds-v0" for parent in folder.parents)


def _used(path: Path):
    """Marks a shared cache file as used, and removes the oldest ones"""
    if path.name == CACHE_NAME:
        return
    try:
        path.touch()
        files = sorted(
            path.parent.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        for old in files[MAX_SHARED_CACHES:]:
            old.unlink(missing_ok=True)
    except OSError:
        pass


def _error(source: str, e: Exception) -> LoadError:
    logging.warning("Error while loading %s: %s", source, e)
    return LoadError(source, f"{type(e).__name__}: {e}", traceback.format_exc())


def _repository_datasets(repository: "BaseRepository"):
    """Yields (source, iterable of datasets) for a repository

    Module-based repositories are scanned module by module, so that errors
    are recorded per module
    """
    import importlib

    from datamaestro.context import Datasets, Repository

    cls = type(repository)
    if not isinstance(repository, Repository) or any(
        getattr(cls, name) is not getattr(Repository, name)
        for name in ("__iter__", "modules", "_modules")
    ):
        # Custom listing of the datasets
        yield repository.id, repository
        return

    for _, _, module in repository._modules():

        def datasets(module=module):
            yield from Datasets(importlib.import_module(module))

        yield module, datasets()


def _fingerprint(context: "Context") -> list:
    """Changes when a repository is updated"""
    from datamaestro.context import Repository

    fingerprint = []
    for repository in context.repositories():
        cls = type(repository)
        item = {
            "repository": f"{cls.__module__}.{cls.__qualname__}",
            "version": repository.version() if hasattr(cls, "version") else None,
        }
        if isinstance(repository, Repository):
            item["mtime"] = max(
                (p.stat().st_mtime for p in repository.configdir.rglob("*.py")),
                default=None,
            )
        fingerprint.append(item)
    return fingerprint
