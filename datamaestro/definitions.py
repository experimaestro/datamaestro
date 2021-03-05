"""
Contains
"""

import sys
import os
import hashlib
import tempfile
import urllib
import shutil
from functools import lru_cache
import logging
import re
import inspect
import urllib.request
from pathlib import Path
from itertools import chain
import importlib
import json
import traceback
from typing import Dict, TypeVar, Union, Callable, TYPE_CHECKING
from experimaestro import argument, constant, Param, Option
from .context import Context, DatafolderPath

if TYPE_CHECKING:
    from datamaestro.download import Download

# --- Objects holding information into classes/function


class DataDefinition:
    """Object that stores the declarative part of a data(set) description
    """

    def __init__(self, repository, t, base=None):
        assert base is None or not inspect.isclass(t)

        # Copy base type and find matching repository
        self.t = t
        self.repository = repository

        # Dataset id (and all aliases)
        self.id = None
        self.base = base

        self.aliases = set()

        self.tags = set(chain(*[c.__datamaestro__.tags for c in self.ancestors()]))
        self.tasks = set(chain(*[c.__datamaestro__.tasks for c in self.ancestors()]))

        self.url = None
        self.description: str = None
        self.name: str = None
        self.version = None

        # Hooks
        self.hooks = {"pre-use": [], "pre-download": []}

        if t.__doc__:
            lines = t.__doc__.split("\n", 2)
            self.name = lines[0]
            if len(lines) > 1:
                assert lines[1].strip() == "", "Second line should be blank"
            if len(lines) > 2:
                self.description = lines[2]

        self.resources: Dict[str, "Download"] = {}

    def register_hook(self, hookname: str, hook: Callable):
        self.hooks[hookname].append(hook)

    @staticmethod
    def repository_relpath(t: type):
        """Find the repository of the current data or dataset definition"""
        map = Context.instance().repositorymap

        fullname = f"{t.__module__}.{t.__name__}"
        components = fullname.split(".")

        current = None
        longest_ix = -1
        repository = None
        for ix, c in enumerate(components):
            current = f"{current}.{c}" if current else c
            if current in map:
                longest_ix = ix
                repository = map[current]

        if repository is None:
            if components[0] == "datamaestro":
                longest_ix = 0
            elif repository is None:
                raise Exception(f"Could not find the repository for {fullname}")

        return repository, components[(longest_ix + 1) :]

    def ancestors(self):
        ancestors = []
        if self.base:
            baseclass = self.base
        else:
            baseclass = self.t

        ancestors.extend(c for c in baseclass.__mro__ if hasattr(c, "__datamaestro__"))

        return ancestors


class DatasetDefinition(DataDefinition):
    """Specialization of DataDefinition for datasets

    A dataset:

    - has a unique ID (and aliases)
    - can be searched for
    - has a data storage space
    - has specific attributes:
        - timestamp: whether the dataset version depends on the time of the download
    """

    def __init__(self, repository, t, base=None):
        super().__init__(repository, t, base=base)
        self.timestamp = False

    def download(self, force=False):
        """Download all the necessary resources"""
        success = True
        for key, resource in self.resources.items():
            try:
                resource.download(force)
            except:
                logging.error("Could not download resource %s", key)
                traceback.print_exc()
                success = False
        return success

    def prepare(self, download=False):
        if download:
            for hook in self.hooks["pre-download"]:
                hook(self)
            if not self.download(False):
                raise Exception("Could not load necessary resources")
        logging.debug("Building with data type %s and dataset %s", self.base, self.t)
        for hook in self.hooks["pre-use"]:
            hook(self)

        resources = {key: value.prepare() for key, value in self.resources.items()}
        dict = self.t(**resources)
        if dict is None:
            name = self.t.__name__
            filename = inspect.getfile(self.t)
            raise Exception(
                f"The dataset method {name} defined in {filename} returned a null object"
            )
        data = self.base(**dict)
        data.id = self.id
        return data

    @property
    def context(self):
        return self.repository.context

    @property
    def path(self) -> Path:
        """Returns the path"""
        path = Path(*self.id.split("."))
        if self.version:
            path = path.with_suffix(".v%s" % self.version)
        return path

    @property
    def datapath(self):
        """Returns the destination path for downloads"""
        return self.repository.datapath / self.path

    @staticmethod
    def find(name: str) -> "DataDefinition":
        """Find a dataset given its name"""
        logging.debug("Searching dataset %s", name)
        for repository in Context.instance().repositories():
            logging.debug("Searching dataset %s in %s", name, repository)
            dataset = repository.search(name)
            if dataset is not None:
                return dataset
        raise Exception("Could not find the dataset %s" % (name))

    def hasfiles(self) -> bool:
        """Returns whether this dataset has files or only includes references"""
        for resource in self.resources.values():
            if resource.hasfiles():
                return True

        return False

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


class FutureAttr:
    """Allows to access a dataset subproperty"""

    def __init__(self, definition, keys):
        self.definition = definition
        self.keys = keys

    def __repr__(self):
        return "[%s].%s" % (self.definition.id, ".".join(self.keys))

    def __call__(self):
        """Returns the value"""
        value = self.definition.prepare()
        for key in self.keys:
            value = getattr(value, key)
        return value

    def __getattr__(self, key):
        return FutureAttr(self.definition, self.keys + [key])

    def download(self, force=False):
        self.definition.download(force)


class DatasetWrapper:
    """Represents a dataset"""

    def __init__(self, annotation, t: type):

        self.t = t

        if annotation.base.__datamaestro__.base:
            # This must be a metadataset
            base = annotation.base.__datamaestro__.base
        else:
            base = annotation.base

        assert base is not None

        repository, components = DataDefinition.repository_relpath(t)

        d = DatasetDefinition(repository, t, base)
        self.__datamaestro__ = d

        # Set some variables
        d.url = annotation.url

        # Builds the ID:
        # Removes module_name.config prefix
        assert (
            components[0] == "config"
        ), f"A @dataset object should be in the .config module (not {t.__module__})"

        path = ".".join(components[1:-1])
        if annotation.id == "":
            # id is empty string = use the module id
            d.id = path
        else:
            d.id = "%s.%s" % (
                path,
                annotation.id or t.__name__.lower().replace("_", "."),
            )

        d.aliases.add(d.id)

    def __call__(self, *args, **kwargs):
        self.t(*args, **kwargs)

    def __getattr__(self, key):
        return FutureAttr(self.__datamaestro__, [key])


# --- Annotations


class DataAnnotation:
    def __call__(self, t):
        # Set some useful members
        self.definition = t.__datamaestro__  # type: DataDefinition
        self.repository = self.definition.repository  # type: Repository
        self.context = (
            self.definition.repository.context if self.definition.repository else None
        )

        # Annotate
        self.annotate()
        return t

    def annotate(self):
        raise NotImplementedError("Method annotate for class %s" % self.__class__)


def hook(name: str):
    """Annotate a method of a DataAnnotation class to be a hook"""

    class HookAnnotation(DataAnnotation):
        def __init__(self, callable: Callable, args, kwargs):
            self.callable = callable
            self.args = args
            self.kwargs = kwargs

        def annotate(self):
            self.definition.register_hook(name, self._hook)

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

        def annotate(self):
            f(self.definition).update(self.tags)

    return Annotation


datatags = DataTagging(lambda d: d.tags)
datatasks = DataTagging(lambda d: d.tasks)

T = TypeVar("T")


def data(description=None):
    if description is not None and not isinstance(description, str):
        raise RuntimeError("@data annotation should be written @data()")

    def annotate(t: T):
        try:
            object.__getattribute__(t, "__datamaestro__")
            raise AssertionError("@data should only be called once")
        except AttributeError:
            pass

        # Determine the data type
        from experimaestro import config

        repository, components = DataDefinition.repository_relpath(t)
        assert (
            components[0] == "data"
        ), f"A @data object should be in the .data module (not {t.__module__})"

        identifier = (
            f"{repository.NAMESPACE if repository else 'datamaestro'}."
            + ".".join(components[1:]).lower()
        )
        t = config(identifier)(t)
        t.__datamaestro__ = DataDefinition(repository, t)
        t.__datamaestro__.id = identifier

        return t

    return annotate


class dataset:
    def __init__(self, base=None, *, timestamp=None, id=None, url=None, size=None):
        """

        Arguments:
            base {[type]} -- The base type (or None if infered from type annotation)

        Keyword Arguments:
            timestamp {bool} -- If the dataset evolves, specify its timestamp (default: None)
            id {[type]} -- [description] (default: {None})
            url {[type]} -- [description] (default: {None})
            size {str} -- The size (should be a parsable format)
        """
        self.base = base

        self.id = id
        self.url = url
        self.meta = False
        self.timestamp = timestamp
        self.size = size

    def __call__(self, t):
        try:
            if self.base is None:
                self.base = t.__annotations__["return"]
            object.__getattribute__(t, "__datamaestro__")
            raise AssertionError("@data should only be called once")
        except AttributeError:
            pass

        dw = DatasetWrapper(self, t)
        dw.__datamaestro__.timestamp = self.timestamp

        return dw


def metadataset(base):
    """Annotation for object/functions which are abstract dataset definitions -- i.e. shared
    by more than one real dataset. This is useful to share tags, urls, etc."""

    def annotate(t):
        try:
            object.__getattribute__(t, "__datamaestro__")
            raise AssertionError("@data should only be called once")
        except AttributeError:
            pass
        t.__datamaestro__ = DataDefinition(None, t, base=base)
        return t

    return annotate
