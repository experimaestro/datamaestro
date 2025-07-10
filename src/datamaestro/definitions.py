#
# Main datamaestro functions and data models
#

import logging
import inspect
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
    argument,
    constant,
    Param,
    Option,
    Config,
    Meta,
)
from typing import Type as TypingType  # noqa: F401 (re-exports)
from experimaestro.core.types import Type  # noqa: F401 (re-exports)

if TYPE_CHECKING:
    from .data import Base, Dataset
    from .context import Repository, Context, DatafolderPath  # noqa: F401 (re-exports)
    from datamaestro.download import Download

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

        return repository, [s.lower() for s in components[(longest_ix + 1) :]]

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
    def _prepare(self) -> "Base":
        ...

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
        """Download all the necessary resources"""
        success = True
        self.prepare()
        logging.info("Materializing %d resources", len(self.ordered_resources))
        for resource in self.ordered_resources:
            try:
                resource.download(force)
            except Exception:
                logging.error("Could not download resource %s", resource)
                traceback.print_exc()
                success = False
                break
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
                components[0]
                == "config"
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

        # Direct creation of the dataset
        if self.base is self.t:
            self.config = self.base.__create_dataset__(self)

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

    def hasfiles(self) -> bool:
        """Returns whether this dataset has files or only includes references"""
        for resource in self.resources.values():
            if resource.hasfiles():
                return True

        return False


# --- Annotations

T = TypeVar("T")


class DataAnnotation:
    def __call__(self, object: T) -> T:
        if isinstance(object, AbstractDataset):
            self.annotate(object)
        else:
            if "__datamaestro__" in object.__dict__:
                self.annotate(object.__datamaestro__)
            else:
                # With configuration objects, add a __datamaestro__ member to the class
                assert issubclass(
                    object, Config
                ), f"{object} cannot be annotated (only dataset or data definitions)"
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
        elif issubclass(dataset, Dataset):
            self.annotate(dataset.__datamaestro__)
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
        self, tags: Union[str, List[str]] = None, tasks: Union[str, List[str]] = None
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
        if inspect.isclass(t) and issubclass(t, Base):
            return t
        return dw


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
