"""Resource system for dataset download and processing pipelines.

This module defines the Resource interface and its concrete subclasses
(FileResource, FolderResource, ValueResource) for managing dataset
download and preprocessing steps as a directed acyclic graph (DAG).
"""

from __future__ import annotations

import json
import logging
import shutil
import warnings
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import IO, Union

from attrs import define

from datamaestro.definitions import AbstractDataset, DatasetAnnotation
from datamaestro.utils import deprecated

logger = logging.getLogger(__name__)

# Module-level deprecation tracking (emit each category only once)
_deprecation_warned: set[str] = set()


def _warn_once(category: str, message: str):
    """Emit a deprecation warning only once per category."""
    if category not in _deprecation_warned:
        _deprecation_warned.add(category)
        warnings.warn(message, DeprecationWarning, stacklevel=3)


# --- State metadata file helpers ---


class ResourceStateFile:
    """Manages the .state.json metadata file for resource states.

    Location: <dataset.datapath>/.state.json

    Format:
        {
          "version": 1,
          "resources": {
            "RESOURCE_NAME": {"state": "none"|"partial"|"complete"},
            ...
          }
        }
    """

    VERSION = 1

    def __init__(self, datapath: Path):
        self._path = datapath / ".state.json"

    def read(self, resource_name: str) -> "ResourceState":
        """Read the state for a resource. Returns NONE if not found."""
        data = self._load()
        entry = data.get("resources", {}).get(resource_name)
        if entry is None:
            return ResourceState.NONE
        return ResourceState(entry["state"])

    def write(self, resource_name: str, state: "ResourceState"):
        """Write the state for a resource (atomic write)."""
        data = self._load()
        if "resources" not in data:
            data["resources"] = {}
        data["resources"][resource_name] = {"state": state.value}
        self._save(data)

    def _load(self) -> dict:
        if self._path.is_file():
            with self._path.open("r") as f:
                return json.load(f)
        return {"version": self.VERSION, "resources": {}}

    def _save(self, data: dict):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(data, f, indent=2)
        tmp.rename(self._path)


# --- ResourceState enum ---


class ResourceState(str, Enum):
    """State of a resource in the preparation pipeline."""

    NONE = "none"
    """Not started / no data on disk."""

    PARTIAL = "partial"
    """Started but incomplete (error during download)."""

    COMPLETE = "complete"
    """Fully available."""


# --- Lazy initialization decorator (backward compat) ---


def initialized(method):
    """Ensure the object is initialized (calls postinit on first use).

    Deprecated: new Resource subclasses should not rely on this pattern.
    """

    def wrapper(self, *args, **kwargs):
        if not self._post:
            self._post = True
            self.postinit()
        return method(self, *args, **kwargs)

    return wrapper


# --- SetupOptions (backward compat) ---


@define(kw_only=True)
class SetupOptions:
    pass


# --- Resource base class ---


class Resource(DatasetAnnotation, ABC):
    """Base class for all dataset resources.

    A resource represents a single step in a dataset preparation pipeline.
    Resources form a DAG: each resource declares its dependencies, and
    the orchestrator ensures they are processed in topological order.

    Usage modes:

    1. Class attribute (preferred)::

        @dataset(url="...")
        class MyDataset(Base):
            DATA = filedownloader("data.csv", "http://...", transient=True)
            PROCESSED = SomeProcessor.from_file(DATA)

    2. Decorator on function (deprecated, backward compat)::

        @filedownloader("data.csv", "http://...")
        @dataset(Base)
        def my_dataset(data): ...

    Two-path system:

    - ``transient_path``: where download/processing writes data
    - ``path``: final location after successful completion

    The framework moves data from ``transient_path`` → ``path`` and then
    marks the resource as COMPLETE. Subclass ``download()`` implementations
    should always write to ``transient_path``.

    State is persisted in a metadata file at::

        <dataset.datapath>/.downloads/.state.json
    """

    def __init__(
        self,
        varname: str | None = None,
        *,
        transient: bool = False,
    ):
        """
        Args:
            varname: Explicit resource name. If None, auto-set from
                class attribute name during binding. Required when
                used as a decorator (backward compat mode).
            transient: If True, this resource's data can be deleted
                after all its dependents reach COMPLETE.
        """
        self.name: str | None = varname
        self._name_explicit: bool = varname is not None
        self.dataset: AbstractDataset | None = None
        self.transient: bool = transient
        self._dependencies: list[Resource] = []
        self._dependents: list[Resource] = []

        # Backward compat: lazy initialization support
        self._post = False

    # ---- Properties ----

    @property
    def can_recover(self) -> bool:
        """Whether partial downloads can be resumed.

        When True and state is PARTIAL, existing data at transient_path
        is preserved on error, allowing the next download() call to
        resume from where it left off.

        When False and state is PARTIAL, data at transient_path is
        deleted and state is reset to NONE.

        Default: False. Subclasses override to enable recovery.
        """
        return False

    @property
    def dependencies(self) -> list[Resource]:
        """Resources that must be COMPLETE before this one can process.

        Populated from constructor arguments. Subclasses with factory
        methods should pass dependency resources to ``__init__`` and
        store them in ``_dependencies``.
        """
        return self._dependencies

    @property
    def dependents(self) -> list[Resource]:
        """Resources that depend on this one (inverse of dependencies).

        Computed by the dataset after all resources are bound.
        Used for eager transient cleanup decisions.
        """
        return self._dependents

    @property
    def path(self) -> Path:
        """Final storage path for this resource's data.

        This is where data lives after successful completion.
        Default: ``dataset.datapath / self.name``

        Subclasses may override to customize (e.g., add file extension).
        """
        return self.dataset.datapath / self.name

    @property
    def transient_path(self) -> Path:
        """Temporary path where download/processing writes data.

        During download(), subclasses write to this path.
        After successful download, the framework moves the data from
        transient_path to path, then marks state as COMPLETE.

        Default: ``dataset.datapath / ".downloads" / self.name``
        """
        return self.dataset.datapath / ".downloads" / self.name

    @property
    def state(self) -> ResourceState:
        """Current state, read from the metadata file.

        If no metadata entry exists, returns NONE.
        """
        if self.dataset is None:
            return ResourceState.NONE
        state_file = ResourceStateFile(self.dataset.datapath)
        return state_file.read(self.name)

    @state.setter
    def state(self, value: ResourceState) -> None:
        """Update state in the metadata file (atomic write)."""
        state_file = ResourceStateFile(self.dataset.datapath)
        state_file.write(self.name, value)

    @property
    def context(self):
        """Application context (from dataset)."""
        return self.dataset.context

    # ---- Abstract methods ----

    @abstractmethod
    def download(self, force: bool = False) -> None:
        """Execute this resource's download/processing step.

        Contract:

        - Called only when all dependencies are COMPLETE.
        - Must write output to ``self.transient_path``.
        - The framework handles moving transient_path → path
          and setting state to COMPLETE after this returns.
        - If force=True, re-execute even if already COMPLETE.

        Note: State management (COMPLETE/PARTIAL/NONE transitions,
        moving transient_path → path) is handled by the framework,
        NOT by the download() implementation.

        Raises:
            Exception: On download/processing failure. The framework
                will handle PARTIAL state based on can_recover.
        """
        ...

    @abstractmethod
    def prepare(self):
        """Return the value for dataset construction.

        Called after download() has completed (state is COMPLETE).
        Return type depends on the resource subclass:

        - FileResource → Path
        - FolderResource → Path
        - ValueResource → resource-specific

        For backward compat with function-based datasets, this value
        is passed as a keyword argument to the dataset function.
        """
        ...

    # ---- Concrete methods ----

    def cleanup(self) -> None:
        """Remove this resource's data from disk.

        Called automatically for transient resources after all
        dependents reach COMPLETE (eager cleanup).

        Default implementation:

        - Deletes self.path (file or directory)
        - Deletes self.transient_path if it exists
        - Sets self.state = NONE

        Subclasses may override for custom cleanup.
        """
        for p in (self.path, self.transient_path):
            if p.exists():
                if p.is_dir():
                    shutil.rmtree(p)
                else:
                    p.unlink()
        self.state = ResourceState.NONE

    def has_files(self) -> bool:
        """Whether this resource produces files on disk.

        Returns False for reference-only resources (e.g., links
        to other datasets, in-memory values).
        Default: True.
        """
        return True

    # Backward compat alias
    def hasfiles(self) -> bool:
        """Deprecated: use has_files() instead."""
        _warn_once("hasfiles", "hasfiles() is deprecated, use has_files()")
        return self.has_files()

    def postinit(self):
        """Legacy lazy initialization hook.

        Deprecated: new Resource subclasses should perform
        initialization in ``__init__`` or ``bind()``.
        """
        pass

    # ---- Binding ----

    def bind(self, name: str, dataset: AbstractDataset) -> None:
        """Bind this resource to a dataset.

        Called by the dataset class machinery during initialization.
        Sets self.name (if not explicitly set via varname) and
        self.dataset. Registers the resource in dataset.resources
        and dataset.ordered_resources.

        For class-based datasets: called by ``@dataset`` when it
        processes class attributes.
        For decorator-based: called by ``annotate()`` (existing protocol).
        """
        if not self._name_explicit:
            self.name = name

        assert self.dataset is None, (
            f"Resource {self.name} is already bound to a dataset"
        )

        if self.name in dataset.resources:
            raise AssertionError(f"Name {self.name} already declared as a resource")

        dataset.resources[self.name] = self
        dataset.ordered_resources.append(self)
        self.dataset = dataset

    def annotate(self, dataset: AbstractDataset) -> None:
        """Register with a dataset (DatasetAnnotation protocol).

        Deprecated for new code. Calls bind() internally.
        """
        _warn_once(
            "annotate",
            "Using resources as decorators is deprecated. "
            "Define them as class attributes instead.",
        )
        self.bind(self.name, dataset)

    def contextualize(self):
        """When using an annotation inline, uses the current
        dataset wrapper object.

        Deprecated: use class-attribute resource definitions instead.
        """
        wrapper = AbstractDataset.processing()
        self.annotate(wrapper)

    def setup(
        self,
        dataset: Union[AbstractDataset],
        options: SetupOptions = None,
    ):
        """Direct way to setup the resource (no annotation).

        Deprecated: use class-attribute resource definitions instead.
        """
        self(dataset)
        return self.prepare()

    # ---- Factory pattern ----

    @classmethod
    def apply(cls, *args, **kwargs) -> "Resource":
        """Factory classmethod for creating resource instances.

        Allows defining shorthand factory functions::

            filedownloader = FileDownloader.apply

        Default implementation: ``return cls(*args, **kwargs)``
        Subclasses may override for custom argument handling.
        """
        return cls(*args, **kwargs)

    # ---- Backward compat: definition property ----

    @property
    def definition(self) -> AbstractDataset | None:
        """Deprecated: use ``dataset`` attribute instead."""
        _warn_once(
            "definition",
            "Resource.definition is deprecated, use Resource.dataset",
        )
        return self.dataset

    # Backward compat: varname property
    @property
    def varname(self) -> str | None:
        """Deprecated: use ``name`` attribute instead."""
        _warn_once(
            "varname",
            "Resource.varname is deprecated, use Resource.name",
        )
        return self.name

    @varname.setter
    def varname(self, value: str | None):
        self.name = value


# --- FileResource ---


class FileResource(Resource):
    """A resource that produces a single file on disk.

    Subclasses implement ``_download()`` to produce the file at the
    given destination (which is ``self.transient_path``).
    """

    def __init__(
        self,
        filename: str,
        *,
        varname: str | None = None,
        transient: bool = False,
    ):
        """
        Args:
            filename: The filename (with extension) for the produced file.
                Used to construct the storage path.
            varname: Explicit resource name. If None, derived from
                filename (extension stripped) or class attribute name.
            transient: See Resource.
        """
        import re

        effective_varname = varname or re.sub(r"\..*$", "", filename)
        super().__init__(varname=effective_varname, transient=transient)
        # Only mark name as explicit if user actually passed varname
        self._name_explicit = varname is not None
        self.filename = filename

    @property
    def path(self) -> Path:
        """Final path to the produced file.

        ``dataset.datapath / self.filename``
        """
        return self.dataset.datapath / self.filename

    @property
    def transient_path(self) -> Path:
        """Temporary path for writing during download.

        ``dataset.datapath / ".downloads" / self.filename``
        """
        return self.dataset.datapath / ".downloads" / self.filename

    def prepare(self) -> Path:
        """Returns self.path."""
        return self.path

    def stream(self) -> IO[bytes] | None:
        """Return a readable byte stream of the file content.

        Returns None if streaming is not supported for this resource.
        Default: returns None. Subclasses may override.

        This allows downstream resources to consume data without
        needing the file to be fully materialized on disk first.
        """
        return None

    def download(self, force: bool = False) -> None:
        """Downloads the file.

        Delegates to ``_download(self.transient_path)``.
        """
        self._download(self.transient_path)

    @abstractmethod
    def _download(self, destination: Path) -> None:
        """Subclass hook: download/produce the file at destination.

        Args:
            destination: The path to write the file to
                (``self.transient_path``).
        """
        ...


# --- FolderResource ---


class FolderResource(Resource):
    """A resource that produces a directory on disk.

    Subclasses implement ``_download()`` to populate the directory at
    the given destination (which is ``self.transient_path``).
    """

    @property
    def path(self) -> Path:
        """Final path to the produced directory.

        ``dataset.datapath / self.name``
        """
        return self.dataset.datapath / self.name

    @property
    def transient_path(self) -> Path:
        """Temporary path for writing during download.

        ``dataset.datapath / ".downloads" / self.name``
        """
        return self.dataset.datapath / ".downloads" / self.name

    def prepare(self) -> Path:
        """Returns self.path."""
        return self.path

    def download(self, force: bool = False) -> None:
        """Downloads/extracts the directory content to transient_path."""
        self._download(self.transient_path)

    @abstractmethod
    def _download(self, destination: Path) -> None:
        """Subclass hook: populate the directory at destination.

        Args:
            destination: The path to write to (``self.transient_path``).
        """
        ...


# --- ValueResource ---


class ValueResource(Resource):
    """A resource that produces an in-memory value (no files on disk).

    Used for resources like HuggingFace dataset handles that don't
    produce local files. The transient_path/path two-path system
    is not used; state tracking is still via metadata file.
    """

    def has_files(self) -> bool:
        return False

    @abstractmethod
    def prepare(self):
        """Return the in-memory value."""
        ...


# --- Deprecated compatibility classes ---


class Download(Resource):
    """Deprecated: use Resource instead."""

    def __init_subclass__(cls):
        _warn_once(
            f"Download-{cls.__name__}",
            f"Download is deprecated ({cls}): use `Resource`",
        )
        return super().__init_subclass__()


# --- reference resource ---


class reference(Resource):
    """References another dataset instead of downloading."""

    def __init__(self, varname=None, reference=None):
        """
        Args:
            varname: The name of the variable.
            reference: Another dataset to reference.
        """
        super().__init__(varname=varname)
        assert reference is not None, "Reference cannot be null"
        self.reference = reference

    def _resolve_reference(self):
        """Resolve the reference to a DatasetWrapper.

        For class-based datasets, the reference is the class itself with
        a __dataset__ attribute pointing to the DatasetWrapper.
        For function-based datasets, the reference is already a DatasetWrapper.
        """
        ref = self.reference
        if hasattr(ref, "__dataset__"):
            return ref.__dataset__
        return ref

    def prepare(self):
        resolved = self._resolve_reference()
        if isinstance(resolved, AbstractDataset):
            return resolved._prepare()
        return resolved.prepare()

    def download(self, force=False):
        resolved = self._resolve_reference()
        if isinstance(resolved, AbstractDataset):
            resolved.download(force)
        elif hasattr(resolved, "__datamaestro__"):
            resolved.__datamaestro__.download(force)
        else:
            resolved.download(force)

    def has_files(self):
        # We don't really have files
        return False


Reference = deprecated("Use @reference instead of @Reference", reference)
