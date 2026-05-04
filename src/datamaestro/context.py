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
from functools import cached_property
from experimaestro.mkdocs.metaloader import Module
from .utils import CachedFile, downloadURL
from .settings import UserSettings, Settings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datamaestro.definitions import AbstractDataset, DatasetWrapper

from importlib.metadata import (
    entry_points as _entry_points,
    version as _version,
    PackageNotFoundError as _PackageNotFoundError,
)


def iter_entry_points(group, name=None):
    """Yield entry points for a given group (and optional name) using importlib.metadata."""
    eps = _entry_points()
    selected = eps.select(group=group)
    if name:
        selected = [ep for ep in selected if ep.name == name]
    for ep in selected:
        yield ep


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
        for entry_point in iter_entry_points("datamaestro.repositories"):
            yield entry_point.load().instance()

    def repository(self, repositoryid):
        if repositoryid is None:
            return None

        entry_points = [
            x for x in iter_entry_points("datamaestro.repositories", repositoryid)
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
        """Get a dataset by ID.

        ``datasetid`` may be a variant-selector form
        (``"pkg.id[k=v,...]"``); the selector is ignored here
        (``Context.dataset`` returns the family wrapper). Use
        :func:`prepare_dataset` to route variant kwargs through to
        ``prepare()``.
        """
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

            # replace (not rename): rename fails on Windows if dest exists
            tmppath.replace(dlpath)

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

    def _ask_user(self, context: Context) -> Path:
        """Interactively ask the user for the datafolder or full path."""
        from rich.console import Console
        from rich.prompt import Prompt

        console = Console()
        console.print(
            f"[bold]Datafolder [cyan]'{self.folderid}'[/cyan]"
            f" is not configured.[/bold]\n"
            f"The full path would be:"
            f" [dim]<datafolder>/{self.path}[/dim]"
        )

        choice = Prompt.ask(
            "Do you want to set [bold](f)[/bold]ull path or [bold](d)[/bold]ata folder",
            choices=["f", "d"],
        )

        if choice == "d":
            folder = Path(
                Prompt.ask(f"Enter base path for datafolder '{self.folderid}'")
            )
            if not folder.exists():
                raise FileNotFoundError(f"The path {folder} does not exist")
            console.print(
                f"To store this setting, run:\n"
                f"  [bold]datamaestro datafolders set"
                f" {self.folderid} {folder}[/bold]"
            )
            context.settings.datafolders[self.folderid] = folder
            return Path(folder) / self.path
        else:
            full_path = Path(Prompt.ask("Enter full path"))
            if not full_path.exists():
                raise FileNotFoundError(f"The path {full_path} does not exist")
            return full_path

    def __call__(self, context: Context) -> Path:
        folder = context.settings.datafolders.get(self.folderid)
        if folder is not None:
            return Path(folder) / self.path

        if not os.isatty(0):
            raise KeyError(
                f"Datafolder '{self.folderid}' is not configured."
                f" Set it with: datamaestro datafolders set"
                f" {self.folderid} PATH"
            )

        return self._ask_user(context)


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
        from .definitions import DatasetWrapper, Dataset
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
                and (issubclass(value, Base) or issubclass(value, Dataset))
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
    def __iter__(self) -> Iterator["AbstractDataset"]: ...

    def search(self, name: str):
        """Search for a dataset in the definitions.

        Accepts either a bare id (``"pkg.id"``) or a variant-selector
        form (``"pkg.id[k=v,...]"``). The selector suffix is stripped
        when matching against aliases; callers that need the parsed
        variant kwargs should use :func:`find_dataset` /
        :func:`prepare_dataset` (which return the resolved config).
        """
        from .variants import split_id_selector

        base_id, _ = split_id_selector(name)
        for dataset in self:
            if base_id in dataset.aliases:
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
        try:
            return _version(cls.__module__)
        except _PackageNotFoundError:
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


def _resolve_dataset_id(dataset_id, context=None, variant=None):
    """Split a possibly-variant dataset id into (wrapper, variant_kwargs).

    ``variant_kwargs`` is ``None`` for flat datasets or when the input
    isn't a string (e.g. a ``DatasetWrapper`` or ``Config`` reference).

    ``variant`` (optional dict) is an alternative to the ``[k=v,...]``
    selector syntax: callers may pass the kwargs directly. Passing both
    a selector in the id and ``variant`` is rejected.
    """
    from .definitions import AbstractDataset, DatasetWrapper
    from .variants import split_id_selector

    if isinstance(dataset_id, DatasetWrapper):
        ds = dataset_id
        selector = ""
        base_id = None
    elif hasattr(dataset_id, "__dataset__"):
        ds = dataset_id.__dataset__
        selector = ""
        base_id = None
    elif isinstance(dataset_id, Config):
        ds = dataset_id.__datamaestro_dataset__
        selector = ""
        base_id = None
    else:
        base_id, selector = split_id_selector(dataset_id)
        ds = AbstractDataset.find(base_id, context=context)

    if selector and variant is not None:
        raise ValueError(
            f"dataset {base_id!r} received both a selector "
            f"{selector!r} and a `variant` kwarg; pass only one"
        )

    variant_kwargs = None
    declared = getattr(ds, "variants", None)
    if declared is None:
        if selector:
            raise ValueError(
                f"dataset {base_id!r} does not declare variants but "
                f"selector {selector!r} was supplied"
            )
        if variant is not None:
            raise ValueError(
                f"dataset does not declare variants but `variant` kwarg "
                f"{variant!r} was supplied"
            )
    else:
        if variant is not None:
            parsed = dict(variant)
        elif selector:
            parsed = declared.parse_selector(selector)
        else:
            parsed = {}
        variant_kwargs = declared.resolve(**parsed)
    return ds, variant_kwargs


def find_dataset(dataset_id: str):
    """Find a dataset given its id.

    Accepts a plain id or a variant-selector form
    (``"pkg.id[k=v,...]"``). The selector only affects downstream
    ``prepare()``; ``find_dataset`` returns the family wrapper.
    """
    ds, _ = _resolve_dataset_id(dataset_id)
    return ds


def prepare_dataset(
    dataset_id: Union[str, "DatasetWrapper", Config],
    context: Optional[Union[Context, Path]] = None,
    *,
    variant: Optional[Dict] = None,
):
    """Find a dataset given its id and download the resources.

    Variants can be selected two ways:

    - Inline in the id: ``prepare_dataset("pkg.id[k=v,...]")``.
    - As a dict kwarg: ``prepare_dataset("pkg.id", variant={"k": v, ...})``.

    Both forms route the kwargs through to the wrapper's :meth:`prepare`
    call (defaults are filled in by the dataset's :class:`Variants`).
    Passing both a selector and ``variant`` raises ``ValueError``.
    """
    match context:
        case Path() | str():
            context = Context(Path(context))

    ds, variant_kwargs = _resolve_dataset_id(
        dataset_id, context=context, variant=variant
    )
    return ds.prepare(download=True, variant_kwargs=variant_kwargs)


def get_dataset(dataset_id: str, *, variant: Optional[Dict] = None):
    """Find a dataset given its id (without downloading).

    Like :func:`prepare_dataset`, accepts either ``"pkg.id[k=v,...]"`` or
    a ``variant={"k": v, ...}`` dict kwarg.
    """
    ds, variant_kwargs = _resolve_dataset_id(dataset_id, variant=variant)
    return ds.prepare(download=False, variant_kwargs=variant_kwargs)
