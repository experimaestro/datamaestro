#
# Main datamaestro functions and data models
#

from __future__ import annotations

import logging
import inspect
import re as _re
import shutil
from pathlib import Path
from itertools import chain
from abc import ABC, abstractmethod
import traceback
from typing import (
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Callable,
    TYPE_CHECKING,
    Union,
    _GenericAlias,
)
from experimaestro import (  # noqa: F401 (re-exports)
    Param,
    Option,
    Config,
    Meta,
)
from typing import Type as TypingType  # noqa: F401 (re-exports)
from experimaestro.core.types import Type  # noqa: F401 (re-exports)

if TYPE_CHECKING:
    from .data import Base
    from .context import Repository, Context, DatafolderPath  # noqa: F401 (re-exports)
    from datamaestro.download import Download, Resource

# --- DAG utilities ---


def topological_sort(resources: dict[str, "Resource"]) -> list["Resource"]:
    """Topological sort of resources by their dependencies.

    Args:
        resources: Dict mapping resource names to Resource instances.

    Returns:
        List of resources in dependency order (dependencies first).

    Raises:
        ValueError: If a cycle is detected in the dependency graph.
    """
    visited: set[str] = set()
    visiting: set[str] = set()  # For cycle detection
    result: list["Resource"] = []

    def visit(resource: "Resource"):
        if resource.name in visited:
            return
        if resource.name in visiting:
            raise ValueError(
                f"Cycle detected in resource dependencies involving {resource.name}"
            )

        visiting.add(resource.name)
        for dep in resource.dependencies:
            visit(dep)
        visiting.discard(resource.name)
        visited.add(resource.name)
        result.append(resource)

    for resource in resources.values():
        visit(resource)

    return result


def _compute_dependents(resources: dict[str, "Resource"]) -> None:
    """Compute the dependents (inverse edges) for all resources."""
    # Clear existing dependents
    for resource in resources.values():
        resource._dependents = []

    # Build inverse edges
    for resource in resources.values():
        for dep in resource.dependencies:
            if resource not in dep._dependents:
                dep._dependents.append(resource)


def _bind_class_resources(cls: type, dataset_wrapper: "AbstractDataset") -> None:
    """Scan class attributes for Resource instances and bind them.

    This is called when a class-based dataset is processed by the
    @dataset decorator. It detects Resource instances defined as
    class attributes and binds them to the dataset.

    Args:
        cls: The dataset class to scan.
        dataset_wrapper: The AbstractDataset to bind resources to.
    """
    from datamaestro.download import Resource

    for attr_name, attr_value in vars(cls).items():
        if isinstance(attr_value, Resource):
            attr_value.bind(attr_name, dataset_wrapper)

    # Build the dependency DAG
    _compute_dependents(dataset_wrapper.resources)

    # Validate: topological sort will raise on cycles
    dataset_wrapper.ordered_resources = topological_sort(dataset_wrapper.resources)


def _delete_path(path: Path) -> None:
    """Delete a file or directory at path."""
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def _move_path(src: Path, dst: Path) -> None:
    """Move a file or directory from src to dst."""
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))


_CAMEL_RE1 = _re.compile(r"([A-Z]+)([A-Z][a-z])")
_CAMEL_RE2 = _re.compile(r"([a-z0-9])([A-Z])")


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case, then lowercase.

    Examples: ProcessedMNIST -> processed_mnist, MyData -> my_data,
    MNIST -> mnist, simple -> simple
    """
    s = _CAMEL_RE1.sub(r"\1_\2", name)
    s = _CAMEL_RE2.sub(r"\1_\2", s)
    return s.lower()


# --- Objects holding information into classes/function


class AbstractData(ABC):
    """Data definition groups common fields between a dataset and a data piece,
    such as tags and tasks"""

    def __init__(self):
        self.tags = set()
        self.tasks = set()

    def ancestors(self):
        """Returns all configuration ancestors"""
        ancestors = []
        ancestors.extend(c for c in self.base.__mro__ if issubclass(c, Config))
        return ancestors


class DataDefinition(AbstractData):
    """Object that stores the declarative part of a data(set) description"""

    def __init__(self, t, base=None):
        assert base is None or not inspect.isclass(t)

        super().__init__()

        # Copy base type and find matching repository
        self.t = t

        # Dataset id (and all aliases)
        self.id = None
        self.base = base

        self.aliases: Set[str] = set()

        self.tags = set(chain(*[c.__datamaestro__.tags for c in self.ancestors()]))
        self.tasks = set(chain(*[c.__datamaestro__.tasks for c in self.ancestors()]))
        self._description: Optional[str] = None

    @property
    def description(self):
        return self._description

    @staticmethod
    def repository_relpath(t: type) -> Tuple["Repository", List[str]]:
        """Find the repository of the current data or dataset definition"""
        from .context import Context  # noqa: F811

        repositorymap = Context.instance().repositorymap

        fullname = f"{t.__module__}.{t.__name__}"
        components = fullname.split(".")

        current: str = ""
        longest_ix = -1
        repository = None
        for ix, c in enumerate(components):
            current = f"{current}.{c}" if current else c
            if (current is not None) and (current in repositorymap):
                longest_ix = ix
                repository = repositorymap[current]

        if repository is None:
            if components[0] == "datamaestro":
                longest_ix = 0

        parts = components[(longest_ix + 1) :]
        # Module components: just lowercase
        # Last component (class/function name): CamelCase → snake_case
        if parts:
            parts = [s.lower() for s in parts[:-1]] + [_camel_to_snake(parts[-1])]
        return repository, parts

    def ancestors(self):
        ancestors = []
        if self.base:
            baseclass = self.base
        else:
            baseclass = self.t

        ancestors.extend(c for c in baseclass.__mro__ if hasattr(c, "__datamaestro__"))

        return ancestors


class AbstractDataset(AbstractData):
    """Specialization of AbstractData for datasets

    A dataset:

    - has a unique ID (and aliases)
    - can be searched for
    - has a data storage space
    - has specific attributes:
        - timestamp: whether the dataset version depends on the time of the download
    """

    name: Optional[str] = None
    """The name of the dataset"""

    url: Optional[str] = None
    """The URL of the dataset"""

    doi: Optional[str] = None
    """The DOI of this dataset"""

    def __init__(self, repository: Optional["Repository"]):
        super().__init__()
        self.repository = repository
        self.timestamp = False
        self.aliases = set()

        # Associated resources
        self.resources: Dict[str, "Download"] = {}
        self.ordered_resources = []

        # Hooks
        # pre-use: before returning the dataset object
        # pre-download: before downloading the dataset
        self.hooks = {"pre-use": [], "pre-download": []}

        self.url = None
        self.version = None

    @property
    def description(self):
        raise NotImplementedError(f"For class {self.__class__}")

    @property
    def configtype(self):
        raise NotImplementedError()

    @property
    def context(self):
        if self.repository is None:
            from datamaestro.context import Context  # noqa: F811

            return Context.instance()
        return self.repository.context

    def prepare(self, download=False) -> "Base":
        ds = self._prepare()
        ds.__datamaestro_dataset__ = self

        if download:
            ds.download()
        return ds

    def register_hook(self, hookname: str, hook: Callable):
        self.hooks[hookname].append(hook)

    @abstractmethod
    def _prepare(self) -> "Base": ...

    def format(self, encoder: str) -> str:
        s = self.prepare()
        if encoder == "normal":
            from .utils import JsonEncoder

            return JsonEncoder().encode(s)
        elif encoder == "xpm":
            from .utils import XPMEncoder

            return XPMEncoder().encode(s)
        else:
            raise Exception("Unhandled encoder: {encoder}")

    def setDataIDs(self, data: Config, id: str):
        """Set nested IDs automatically"""
        from datamaestro.data import Base

        if isinstance(data, Base):
            try:
                if data.id:
                    # There is already an ID, skip this
                    # and the descendants
                    return
            except KeyError:
                pass

            if self.repository is None:
                data.id = id
            else:
                data.id = f"{id}@{self.repository.name}"
        for key, value in data.__xpm__.values.items():
            if isinstance(value, Config):
                self.setDataIDs(value, f"{id}.{key}")

    def download(self, force=False):
        """Download all the necessary resources.

        Uses DAG-based topological ordering and the two-path system:
        1. Acquire exclusive lock (.state.lock)
        2. Resource writes to transient_path (under .downloads/)
        3. Framework moves transient_path → path (main folder)
        4. State marked COMPLETE
        5. Transient dependencies cleaned up eagerly
        6. .downloads/ directory removed after all resources complete
        7. Release lock
        """
        import fcntl

        from datamaestro.download import ResourceState

        self.prepare()
        logging.info(
            "Materializing %d resources",
            len(self.ordered_resources),
        )

        self.datapath.mkdir(parents=True, exist_ok=True)
        lock_path = self.datapath / ".state.lock"
        lock_file = lock_path.open("w")
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            success = self._download_locked(force, ResourceState)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()

        return success

    def _download_locked(self, force, ResourceState):
        """Inner download logic, called while holding .state.lock."""
        success = True

        for resource in self.ordered_resources:
            # Step 1: Check state
            current_state = resource.state

            if current_state == ResourceState.COMPLETE and not force:
                # Verify files are actually present on disk
                if resource.has_files() and not resource.path.exists():
                    logging.warning(
                        "Resource %s marked COMPLETE but files "
                        "missing at %s — re-downloading",
                        resource.name,
                        resource.path,
                    )
                    resource.state = ResourceState.NONE
                    current_state = ResourceState.NONE
                else:
                    continue

            # Adopt pre-existing files (old downloads without state file)
            if (
                current_state == ResourceState.NONE
                and not force
                and resource.has_files()
                and resource.path.exists()
            ):
                logging.info(
                    "Resource %s already exists at %s — marking COMPLETE",
                    resource.name,
                    resource.path,
                )
                resource.state = ResourceState.COMPLETE
                continue

            if current_state == ResourceState.PARTIAL:
                if not resource.can_recover:
                    _delete_path(resource.transient_path)
                    resource.state = ResourceState.NONE

            # Verify all dependencies are COMPLETE
            for dep in resource.dependencies:
                if dep.state != ResourceState.COMPLETE:
                    logging.error(
                        "Dependency %s of %s is not COMPLETE",
                        dep.name,
                        resource.name,
                    )
                    return False

            # Step 2-4: Download with framework-managed state
            try:
                resource.download(force=force)

                # Move transient -> final, mark COMPLETE
                if resource.has_files():
                    _move_path(resource.transient_path, resource.path)
                resource.state = ResourceState.COMPLETE

            except Exception:
                logging.error("Could not download resource %s", resource)
                traceback.print_exc()

                # Handle PARTIAL state
                if resource.has_files() and resource.transient_path.exists():
                    if resource.can_recover:
                        resource.state = ResourceState.PARTIAL
                    else:
                        _delete_path(resource.transient_path)
                        resource.state = ResourceState.NONE
                success = False
                break

            # Step 5: Eager transient cleanup
            for dep in resource.dependencies:
                if dep.transient and all(
                    d.state == ResourceState.COMPLETE for d in dep.dependents
                ):
                    dep.cleanup()

        # Step 6: Remove .downloads/ directory after success
        if success:
            downloads_dir = self.datapath / ".downloads"
            if downloads_dir.is_dir():
                shutil.rmtree(downloads_dir)

        return success

    @staticmethod
    def find(name: str, context: Optional["Context"] = None) -> "DataDefinition":
        """Find a dataset given its name"""
        from datamaestro.context import Context  # noqa: F811

        context = Context.instance() if context is None else context

        logging.debug("Searching dataset %s", name)
        for repository in Context.instance().repositories():
            logging.debug("Searching dataset %s in %s", name, repository)
            dataset = repository.search(name)
            if dataset is not None:
                return dataset
        raise Exception("Could not find the dataset %s" % (name))


class FutureAttr:
    """Allows to access a dataset sub-property"""

    def __init__(self, dataset, keys):
        self.dataset = dataset
        self.keys = keys

    def __repr__(self):
        return "[%s].%s" % (self.dataset.id, ".".join(self.keys))

    def __call__(self):
        """Returns the value"""
        value = self.dataset.prepare()
        for key in self.keys:
            value = getattr(value, key)
        return value

    def __getattr__(self, key):
        return FutureAttr(self.dataset, self.keys + [key])

    def download(self, force=False):
        self.dataset.download(force)


class DatasetWrapper(AbstractDataset):
    """Wraps an annotated method into a dataset

    This is the standard way to define a dataset in datamaestro through
    annotations (otherwise, derive from `AbstractDataset`).
    """

    def __init__(self, annotation: "dataset", t: type):
        self.config = None
        self.repository: Optional[Repository] = None
        self.t = t
        self.base = annotation.base
        assert self.base is not None, f"Could not set the Config type for {t}"

        repository, components = DataDefinition.repository_relpath(t)
        super().__init__(repository)

        self.module_name = None
        if repository is None:
            # Try to find the module name
            self.module_name, _ = t.__module__.split(".", 1)

        # Set some variables
        self.url = annotation.url
        self.doi = annotation.doi
        self.as_prepare = annotation.as_prepare

        # Builds the ID:
        # Removes module_name.config prefix
        if (
            (annotation.id is None)
            or (annotation.id == "")
            or ("." not in annotation.id)
            or (annotation.id[0] == ".")
        ):
            # Computes an ID
            assert (
                # id is empty string = use the module id
                components[0] == "config"
            ), (
                "A @dataset without `id` should be in the "
                f".config module (not {t.__module__})"
            )

            if annotation.id is None:
                # There is nothing, use the full path
                path = ".".join(components[1:])
            else:
                # Replace
                path = ".".join(components[1:-1])
                if annotation.id != "":
                    path = f"{path}.{annotation.id}"

            self.id = path
        else:
            # Use the provided ID
            self.id = annotation.id

        self.aliases.add(self.id)

        # Get the documentation
        self._name = None
        self._description = None

    @property
    def name(self):
        self._process_doc()
        return self._name

    @property
    def description(self):
        self._process_doc()
        return self._description

    def _process_doc(self):
        if self._description is None:
            if self.t.__doc__:
                lines = self.t.__doc__.split("\n")
                self._name = lines[0]
                if len(lines) > 1:
                    assert lines[1].strip() == "", "Second line should be blank"
                if len(lines) > 2:
                    # Remove the common indent
                    lines = [line.rstrip() for line in lines[2:]]
                    minindent = max(
                        next(idx for idx, chr in enumerate(s) if not chr.isspace())
                        for s in lines
                        if len(s) > 0
                    )
                    self._description = "\n".join(
                        s[minindent:] if len(s) > 0 else "" for s in lines
                    )
            else:
                self._name = ""
                self._description = ""

    @property
    def configtype(self):
        return self.base

    def __getattr__(self, key):
        """Returns a pointer to a potential attribute"""
        return FutureAttr(self, [key])

    def download(self, force=False):
        if self.base is self.t:
            self._prepare()
        return super().download(force=force)

    def _prepare(self) -> "Base":
        if self.config is not None:
            return self.config

        # Dataset subclass with config() method
        if inspect.isclass(self.t) and issubclass(self.t, Dataset):
            instance = self.t()
            self.config = instance.config()

        # Direct creation of the dataset
        elif self.base is self.t:
            self.config = self.base.__create_dataset__(self)

        elif hasattr(self.t, "__create_dataset__"):
            # Class-based dataset with metadataset or different base
            self.config = self.t.__create_dataset__(self)

        else:
            # Construct the object
            if self.as_prepare:
                result = self.t(self, None)
            else:
                resources = {
                    key: value.prepare() for key, value in self.resources.items()
                }
                result = self.t(**resources)

            if result is None:
                raise RuntimeError(f"{self.base} did not return any resource")

            # Download resources
            logging.debug(
                "Building with data type %s and dataset %s", self.base, self.t
            )
            for hook in self.hooks["pre-use"]:
                hook(self)

            if result is None:
                name = self.t.__name__
                filename = inspect.getfile(self.t)
                raise Exception(
                    f"The dataset method {name} defined in "
                    f"{filename} returned a null object"
                )

            if isinstance(result, dict):
                self.config = self.base.C(**result)
            elif isinstance(result, self.base):
                self.config = result
            else:
                name = self.t.__name__
                filename = inspect.getfile(self.t)
                raise RuntimeError(
                    f"The dataset method {name} defined in "
                    f"{filename} returned an object of type {type(dict)}"
                )

        # Setup ourself
        self.config.__datamaestro_dataset__ = self

        # Set the ids
        self.setDataIDs(self.config, self.id)

        return self.config

    __call__ = _prepare

    @property
    def _path(self) -> Path:
        """Returns a unique relative path for this dataset"""
        path = Path(*self.id.split("."))
        if self.version:
            path = path.with_suffix(".v%s" % self.version)
        return path

    @property
    def datapath(self):
        """Returns the destination path for downloads"""
        if self.repository is not None:
            return self.repository.datapath / self._path

        # No repository, use __custom__/[MODULE NAME]
        path = self.context.datapath / "__custom__" / self.module_name / self._path

        return path

    def has_files(self) -> bool:
        """Returns whether this dataset has files or only includes references."""
        for resource in self.resources.values():
            if resource.has_files():
                return True
        return False

    def hasfiles(self) -> bool:
        """Deprecated: use has_files() instead."""
        import warnings

        warnings.warn(
            "hasfiles() is deprecated, use has_files()",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.has_files()


# --- Annotations

T = TypeVar("T")


class DataAnnotation:
    def __call__(self, object: T) -> T:
        if isinstance(object, AbstractDataset):
            self.annotate(object)
        else:
            if "__datamaestro__" in object.__dict__:
                self.annotate(object.__datamaestro__)
            elif "__dataset__" in object.__dict__:
                # Dataset subclass decorated with @dataset
                self.annotate(object.__dataset__)
            else:
                # With configuration objects, add a __datamaestro__ member to the class
                assert issubclass(object, Config), (
                    f"{object} cannot be annotated (only dataset or data definitions)"
                )
                if "__datamaestro__" not in object.__dict__:
                    object.__datamaestro__ = AbstractData()
                self.annotate(object.__datamaestro__)

        return object

    def annotate(self, data: AbstractData):
        raise NotImplementedError("Method annotate for class %s" % self.__class__)


class DatasetAnnotation:
    """Base class for all annotations"""

    def __call__(self, dataset: AbstractDataset):
        if isinstance(dataset, AbstractDataset):
            self.annotate(dataset)
        elif hasattr(dataset, "__dataset__"):
            self.annotate(dataset.__dataset__)
        else:
            raise RuntimeError(
                f"Only datasets can be annotated with {self}, "
                f"but {dataset} is not a dataset"
            )

        return dataset

    def annotate(self, dataset: AbstractDataset):
        raise NotImplementedError("Method annotate for class %s" % self.__class__)


def hook(name: str):
    """Annotate a method of a DatasetAnnotation class to be a hook"""

    class HookAnnotation(DatasetAnnotation):
        def __init__(self, callable: Callable, args, kwargs):
            self.callable = callable
            self.args = args
            self.kwargs = kwargs

        def annotate(self, dataset):
            dataset.register_hook(name, self._hook)

        def _hook(self, definition: DataDefinition):
            self.callable(definition, *self.args, **self.kwargs)

    def annotate(callable: Callable):
        def collect(*args, **kwargs):
            return HookAnnotation(callable, args, kwargs)

        return collect

    return annotate


def DataTagging(f):
    class Annotation(DataAnnotation):
        """Define tags in a data definition"""

        def __init__(self, *tags):
            self.tags = tags

        def annotate(self, metadata):
            f(metadata).update(self.tags)

    return Annotation


datatags = DataTagging(lambda d: d.tags)
datatasks = DataTagging(lambda d: d.tasks)


class metadata:
    def __init__(
        self,
        tags: Union[str, List[str]] = None,
        tasks: Union[str, List[str]] = None,
    ):
        pass

    def __call__(self, object: type):
        # FIXME: todo
        return object


class dataset:
    """Dataset decorator

    Meta-datasets are not associated with any base type.

    :param base: The base type (or None if inferred from type annotation).
    :param timestamp: If the dataset evolves, specify its timestamp.
    :param id: Gives the full ID of the dataset if it contains a '.',
        the last component if not containing a '.', or the last components
        if starting with '.'
    :param url: The URL associated with the dataset.
    :param size: The size of the dataset (should be a parsable format).
    :param doi: The DOI of the corresponding paper.
    :param as_prepare: Resources are setup within the method itself
    """

    def __init__(
        self,
        base=None,
        *,
        timestamp: str | None = None,
        id: None | str = None,
        url: None | str = None,
        size: None | int | str = None,
        doi: None | str = None,
        as_prepare: bool = False,
    ):
        if hasattr(base, "__datamaestro__") and isinstance(
            base.__datamaestro__, metadataset
        ):
            self.base = base.__datamaestro__.base
        else:
            self.base = base

        self.id = id
        self.url = url
        self.meta = False
        self.timestamp = timestamp
        self.size = size
        self.doi = doi
        self.as_prepare = as_prepare

    def __call__(self, t):
        from datamaestro.data import Base

        try:
            if self.base is None:
                if inspect.isclass(t) and issubclass(t, Base):
                    self.base = t
                elif inspect.isclass(t) and issubclass(t, Dataset):
                    # Infer base from config() return annotation
                    try:
                        config_method = t.config
                        return_type = config_method.__annotations__["return"]
                        if isinstance(return_type, _GenericAlias):
                            return_type = return_type.__origin__
                        self.base = return_type
                    except (KeyError, AttributeError):
                        logging.warning("No return annotation on config() in %s", t)
                        raise
                else:
                    try:
                        # Get type from return annotation
                        return_type = t.__annotations__["return"]
                        if isinstance(return_type, _GenericAlias):
                            return_type = return_type.__origin__
                        self.base = return_type
                    except KeyError:
                        logging.warning("No return annotation in %s", t)
                        raise
            object.__getattribute__(t, "__datamaestro__")
            raise AssertionError("@data should only be called once")
        except AttributeError:
            pass
        dw = DatasetWrapper(self, t)
        t.__dataset__ = dw

        # For class-based datasets, scan for Resource class attributes
        if inspect.isclass(t) and (issubclass(t, Base) or issubclass(t, Dataset)):
            _bind_class_resources(t, dw)
            return t
        return dw


class Dataset(ABC):
    """Base class for simplified dataset definitions.

    Inherit from this class and use the ``@dataset`` decorator.
    Resources are defined as class attributes and accessed via ``self``.

    Example::

        @dataset(url="http://yann.lecun.com/exdb/mnist/")
        class MNIST(Dataset):
            \"\"\"The MNIST database of handwritten digits.\"\"\"

            TRAIN_IMAGES = FileDownloader("train.idx", "http://...")
            TEST_IMAGES = FileDownloader("test.idx", "http://...")

            def config(self) -> ImageClassification:
                return ImageClassification.C(
                    train=IDX(path=self.TRAIN_IMAGES.path),
                    test=IDX(path=self.TEST_IMAGES.path),
                )
    """

    @abstractmethod
    def config(self) -> "Base":
        """Create and return the dataset configuration.

        Override this method to construct and return the data object.
        Resources are accessible via ``self.RESOURCE_NAME.path`` or
        ``self.RESOURCE_NAME.prepare()``.

        Returns:
            A Config instance (typically created via ``SomeType.C(...)``).
        """
        ...


class metadataset(AbstractDataset):
    """Annotation for object/functions which are abstract dataset definitions

    i.e. shared by more than one real dataset. This is useful to share tags,
    urls, etc.
    """

    def __init__(self, base):
        super().__init__(None)
        self.base = base

    def __call__(self, t):
        try:
            object.__getattribute__(t, "__datamaestro__")
            raise AssertionError("@data should only be called once")
        except AttributeError:
            pass
        t.__datamaestro__ = self
        return t

    _prepare = None
