from pathlib import Path
from typing import Iterable, Iterator, Dict, Optional, Union
import importlib
import os
import hashlib
import logging
import inspect
import json
from abc import ABC, abstractmethod
from experimaestro import Config
import pkg_resources
from experimaestro.compat import cached_property
from experimaestro.mkdocs.metaloader import Module
from .utils import CachedFile, downloadURL
from .settings import UserSettings, Settings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datamaestro.definitions import AbstractDataset, DatasetWrapper


class Compression:
    @staticmethod
    def extension(definition):
        if not definition:
            return ""
        if definition == "gzip":
            return ".gz"

        raise Exception("Not handled compression definition: %s" % definition)


class Context:
    """
    Represents the application context
    """

    MAINDIR = Path(os.environ.get("DATAMAESTRO_DIR", "~/datamaestro")).expanduser()

    INSTANCE = None

    """Main settings"""

    def __init__(self, path: Path = None):
        assert not Context.INSTANCE

        Context.INSTANCE = self
        self._path = path or Context.MAINDIR
        self._dpath = Path(__file__).parents[1]
        self._repository = None
        # self.registry = Registry(self.datapath / "registry.yaml")
        self.keep_downloads = False
        self.traceback = False

        # Read global preferences
        self.settings = Settings.load(self._path / "settings.json")

        # Read user preferences
        path = Path("~").expanduser() / ".config" / "datamaestro" / "user.json"
        self.user_settings = UserSettings.load(path)

    @staticmethod
    def instance():
        if Context.INSTANCE is None:
            Context.INSTANCE = Context()
        return Context.INSTANCE

    @staticmethod
    def remote(host, pythonpath, datapath=None):
        """Create a remote context by connecting to a given host"""
        from experimaestro.rpyc import client

        client = client(hostname=host, pythonpath=pythonpath).__enter__()
        context = client.connection.modules.datamaestro.context.Context(datapath)
        return context

    @staticmethod
    def frompath(path: Path):
        context = Context.instance()

        class ContextManager:
            def __enter__(self):
                self.previous = Context.INSTANCE
                return context

            def __exit__(self, exc_type, exc_val, exc_tb):
                Context.INSTANCE = self.previous

        return ContextManager()

    @property
    def datapath(self):
        return self._path.joinpath("data")

    @property
    def cachepath(self) -> Path:
        return self._path.joinpath("cache")

    @cached_property
    def repositorymap(self) -> Dict[str, "Repository"]:
        return {
            repository.basemodule(): repository
            for repository in self.repositories()
            if repository.basemodule() is not None
        }

    def repositories(self) -> Iterable["Repository"]:
        """Returns an iterator over repositories"""
        for entry_point in pkg_resources.iter_entry_points("datamaestro.repositories"):
            yield entry_point.load().instance()

    def repository(self, repositoryid):
        if repositoryid is None:
            return None

        entry_points = [
            x
            for x in pkg_resources.iter_entry_points(
                "datamaestro.repositories", repositoryid
            )
        ]
        if not entry_points:
            raise Exception("No datasets repository named %s", repositoryid)
        if len(entry_points) > 1:
            raise Exception(
                "Too many datasets repository named %s (%d)"
                % (repositoryid, len(entry_points))
            )
        return entry_points[0].load()(self)

    @property
    def running_test(self):
        return "PYTEST_CURRENT_TEST" in os.environ

    def datasets(self):
        """Returns an iterator over all files"""
        for repository in self.repositories():
            for dataset in repository:
                yield dataset

    def dataset(self, datasetid) -> "AbstractDataset":
        """Get a dataset by ID"""
        for repository in self.repositories():
            dataset = repository.search(datasetid)
            if dataset is not None:
                return dataset

        raise Exception("Dataset {} not found".format(datasetid))

    def downloadURL(self, url, size: int = None):
        """Downloads an URL

        Args:
            url (str): The URL to download
            size (str): The size if known (in bytes)
        """

        self.cachepath.mkdir(exist_ok=True)

        def getPaths(hasher):
            """Returns a cache file path"""
            path = self.cachepath.joinpath(hasher.hexdigest())
            urlpath = path.with_suffix(".url")
            dlpath = path.with_suffix(".dl")

            if urlpath.is_file():
                if urlpath.read_text() != url:
                    # TODO: do something better
                    raise Exception(
                        "Cached URL hash does not match. Clear cache to resolve"
                    )
            return urlpath, dlpath

        hasher = hashlib.sha256(json.dumps(url).encode("utf-8"))

        urlpath, dlpath = getPaths(hasher)
        urlpath.write_text(url)

        if dlpath.is_file():
            logging.debug("Using cached file %s for %s", dlpath, url)
        else:
            logging.info("Downloading %s", url)
            tmppath = dlpath.with_suffix(".tmp")

            downloadURL(url, tmppath, tmppath.is_file(), size=size)

            # Now, rename to original
            tmppath.rename(dlpath)

        return CachedFile(dlpath, keep=self.keep_downloads, others=[urlpath])

    def ask(self, question: str, options: Dict[str, str]):
        """Ask a question to the user"""
        print(question)  # noqa: T201
        answer = None
        while answer not in options:
            answer = input().strip().lower()
        return options[answer]


class ResolvablePath:
    """An object than can be resolved into a Path"""

    @staticmethod
    def resolve(context, path):
        if isinstance(path, ResolvablePath):
            return path(context)
        return Path(path)

    """Class that returns a path"""

    def __call__(self, context: Context) -> Path:
        raise NotImplementedError()


class DatafolderPath(ResolvablePath):
    def __init__(self, folderid, path):
        self.folderid = folderid
        self.path = path

    def __str__(self):
        return "datafolder-path({folderid}):{path}".format(**self.__dict__)

    def __call__(self, context: Context) -> Path:
        return Path(context.settings.datafolders[self.folderid]) / self.path


class Datasets(Iterable["AbstractDataset"]):
    """A set of datasets contained within a Python module"""

    def __init__(self, module: Module):
        """Initialize with a module"""
        self.module = module
        self._title = None
        self._description = None

    @property
    def id(self):
        return ".".join(self.module.__name__.split(".", 2)[2:])

    @property
    def title(self):
        self._getdoc()
        return self._title

    @property
    def description(self):
        self._getdoc()
        return self._description

    def _getdoc(self):
        if self._title is not None:
            return

        if not self.module.__doc__:
            self._title = ""
            self._description = ""
            return

        intitle = True
        title = []
        description = []
        for line in self.module.__doc__.split("\n"):
            if line.strip() == "" and intitle:
                intitle = False
            else:
                (title if intitle else description).append(line)

        self._title = " ".join(title)
        self._description = "\n".join(description)

    def __iter__(self) -> Iterable["AbstractDataset"]:
        from .definitions import DatasetWrapper
        from datamaestro.data import Base

        # Iterates over defined symbols
        for key, value in self.module.__dict__.items():
            # Ensures it is annotated
            if isinstance(value, DatasetWrapper):
                # Ensure it comes from the module
                if self.module.__name__ == value.t.__module__:
                    yield value
            elif (
                inspect.isclass(value)
                and issubclass(value, Base)
                and hasattr(value, "__dataset__")
            ):
                if self.module.__name__ == value.__module__:
                    yield value.__dataset__


class BaseRepository(ABC):
    """A repository groups a set of datasets and their corresponding specific
    handlers (downloading, filtering, etc.)"""

    def __init__(self, context: Context):
        self.context = context
        p = inspect.getabsfile(self.__class__)
        self.basedir = Path(p).parent

    @abstractmethod
    def __iter__(self) -> Iterator["AbstractDataset"]:
        ...

    def search(self, name: str):
        """Search for a dataset in the definitions"""
        for dataset in self:
            if name in dataset.aliases:
                return dataset

    @classmethod
    def instance(cls, context=None):
        try:
            return cls.__getattribute__(cls, "INSTANCE")
        except AttributeError:
            return cls(context if context else Context.instance())

    @classmethod
    def basemodule(cls):
        return cls.__module__

    @property
    def generatedpath(self):
        return self.basedir / "generated"

    @property
    def datapath(self):
        return self.context.datapath.joinpath(self.id)

    @property
    def extrapath(self):
        """Path to the directory containing extra configuration files"""
        return self.basedir / "data"


class Repository(BaseRepository):
    """(deprecated) Repository where datasets are located in __module__.config"""

    def __init__(self, context: Context):
        """Initialize a new repository

        :param context: The dataset main context
        :param basedir: The base directory of the repository
            (by default, the same as the repository class)
        """
        super().__init__(context)
        self.context = context
        self.configdir = self.basedir.joinpath("config")
        self.id = self.__class__.NAMESPACE
        self.name = self.id
        self.module = self.__class__.__module__
        self.__class__.INSTANCE = self

    @classmethod
    def version(cls):
        from pkg_resources import get_distribution, DistributionNotFound

        try:
            return get_distribution(cls.__module__).version
        except DistributionNotFound:
            return None

    def __repr__(self):
        return "Repository(%s)" % self.basedir

    def __hash__(self):
        return self.basedir.__hash__()

    def __eq__(self, other):
        assert isinstance(other, Repository)
        return self.basedir == other.basedir

    def datasets(self, candidate: str):
        """Returns the dataset candidates from a module"""
        try:
            module = importlib.import_module("%s.config.%s" % (self.module, candidate))
        except ModuleNotFoundError:
            return None
        return Datasets(module)

    def modules(self) -> Iterator["Module"]:
        """Iterates over all modules in this repository"""
        for _, fid, package in self._modules():
            try:
                module = importlib.import_module(package)
                yield Datasets(module)
            except Exception as e:
                import traceback

                traceback.print_exc()
                logging.error("Error while loading module %s: %s", package, e)

    def _modules(self):
        """Iterate over modules (without parsing them)"""
        for path in self.configdir.rglob("*.py"):
            try:
                relpath = path.relative_to(self.configdir)
                c = [p.name for p in relpath.parents][:-1][::-1]
                if path.name != "__init__.py":
                    c.append(path.stem)
                fid = ".".join(c)

                package = ".".join([self.module, "config", *c])

                yield self, fid, package
            except Exception as e:
                import traceback

                traceback.print_exc()
                logging.error("Error while reading definitions file %s: %s", path, e)

    def __iter__(self) -> Iterator["AbstractDataset"]:
        """Iterates over all datasets in this repository"""
        for datasets in self.modules():
            for dataset in datasets:
                yield dataset


def find_dataset(dataset_id: str):
    """Find a dataset given its id"""
    from .definitions import AbstractDataset

    return AbstractDataset.find(dataset_id)


def prepare_dataset(
    dataset_id: Union[str, "DatasetWrapper", Config],
    context: Optional[Union[Context, Path]] = None,
):
    """Find a dataset given its id and download the resources"""
    from .definitions import AbstractDataset, DatasetWrapper

    match context:
        case Path() | str():
            context = Context(Path(context))

    if isinstance(dataset_id, DatasetWrapper):
        ds = dataset_id
    elif isinstance(dataset_id, Config):
        ds = dataset_id.__datamaestro_dataset__
    else:
        ds = AbstractDataset.find(dataset_id, context=context)

    return ds.prepare(download=True)


def get_dataset(dataset_id: str):
    """Find a dataset given its id"""
    from .definitions import AbstractDataset

    ds = AbstractDataset.find(dataset_id)
    return ds.prepare(download=False)
