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
import yaml
from typing import Union
from .experimaestro import Argument, Type
from .context import Context, DownloadReportHook

class DataDefinition():
    """Object that stores the declarative part of a data(set) description
    """
    def __init__(self, t):
        # Copy base type and find matching repository
        self.t = t
        module = importlib.import_module(t.__module__.split(".",1)[0])
        self.repository = module.Repository.instance() if module.__name__ != "datamaestro" else None

        # Dataset id (and all aliases)
        self.id = None
        self.aliases = set()

        self.tags = set()
        self.tasks = set()

        self.url = None
        self.description = None
        self.name = None
        self.version = None
        if t.__doc__:
            lines = t.__doc__.split("\n", 3)
            self.name = lines[0]
            if len(lines) > 1:
                assert lines[1].strip() == "", "Second line should be blank"
            if len(lines) > 2:
                self.description = lines[2]

        self.resources = {}

    def update(self, base):
        self.tags.update(base.tags)
        self.tasks.update(base.tasks)
        for key, resource in base.resources.items():
            if key not in self.resources:
                self.resources[key] = value

class DatasetDefinition(DataDefinition):
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
        if download and not self.download(False):
            raise Exception("Could not load necessary resources")
        logging.debug("Building with data type %s and dataset %s", self.base, self.t)
        data = self.base(**self.t(**self.resources))
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


    @property
    def datadir(self):
        """Path containing real data"""
        datapath = self.module.repository.datapath
        if "datapath" in self:
            steps = self.id.split(".")
            steps.extend(self["datapath"].split("/"))
        else:
            steps = self.id.split(".")
        return datapath.joinpath(*steps)

    @property
    def destpath(self):
        """Returns the destination path for downloads"""
        return self.repository.downloadpath.joinpath(self.path)



class DataAnnotation():
  def __call__(self, t):
    # Set some useful members
    self.definition = t.__datamaestro__
    self.repository = self.definition.repository
    self.context = self.definition.repository.context if self.definition.repository else None

    # Annotate
    self.annotate()
    return t

  def annotate(self):
    raise NotImplementedError("Method annotate for class %s" % self.__class__)


def DataTagging(f): 
  class _Annotation(DataAnnotation):
    """Define tags in a data definition"""
    def __init__(self, *tags):
      self.tags = tags

    def annotate(self):
      f(self.definition).update(self.tags)
  return _Annotation

DataTags = DataTagging(lambda d: d.tags)
DataTasks = DataTagging(lambda d: d.tasks)

def Data(description=None): 
    def annotate(t):
        try:
            object.__getattribute__(t, "__datamaestro__")
            raise AssertionError("@Data should only be called once")
        except AttributeError:
            pass

        # Determine the data type
        from .experimaestro import Type
        module, data, path = ("%s.%s" % (t.__module__, t.__name__)).split(".", 2)
        assert data == "data", "A @Data object should be in the .data module (not %s.%s)" % (module, data)
        identifier = "%s.%s" % (module, path.lower())
        t = Type(identifier)(t)

        t.__datamaestro__ = DataDefinition(t)
        t.__datamaestro__.id = identifier

        return t
    return annotate


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
    def __init__(self, annotation, t):
        self.t = t
        d = DatasetDefinition(t)
        self.__datamaestro__ = d
        d.base = annotation.base
        d.update(annotation.base.__datamaestro__)
        
        # Removes module_name.config prefix
        path = t.__module__.split(".", 2)[2]
        d.id = "%s.%s" % (path, t.__name__.lower())
        d.aliases.add(d.id)

    def __call__(self, *args, **kwargs):
        self.t(*args, **kwargs)

    def __getattr__(self, key):
        return FutureAttr(self.__datamaestro__, [key])

class Dataset():
    def __init__(self, base, url=None): 
        self.base = base

    def __call__(self, t):
        try:
            object.__getattribute__(t, "__datamaestro__")
            raise AssertionError("@Data should only be called once")
        except AttributeError:
            pass
        
        return DatasetWrapper(self, t)

def datasets(module):
    for key, value in module.__dict__.items():
        if hasattr(value, "__datamaestro__"):
            if value.__datamaestro__.aliases:
                yield value

class Repository:
    """A repository regroup a set of datasets and their corresponding specific handlers (downloading, filtering, etc.)"""

    def __init__(self, context: Context):
        """Initialize a new repository

        :param context: The dataset main context
        :param basedir: The base directory of the repository
            (by default, the same as the repository class)
        """
        self.context = context
        p = inspect.getabsfile(self.__class__)
        self.basedir = Path(p).parent
        self.configdir = self.basedir.joinpath("config")
        self.id = self.__class__.NAMESPACE
        self.name = self.id
        self.module = self.__class__.__module__
        self.__class__.INSTANCE = self

    @classmethod
    def instance(cls, context=None):
        try:
            return cls.__getattribute__(cls, "INSTANCE")
        except AttributeError:
            return cls(context if context else Context.instance())


    def __repr__(self):
        return "Repository(%s)" % self.basedir

    def __hash__(self):
        return self.basedir.__hash__()

    def __eq__(self, other):
        assert isinstance(other, Repository)
        return self.basedir == other.basedir

    def search(self, name: str):
        """Search for a dataset in the definitions"""
        logging.debug("Searching for %s in %s", name, self.configdir)


        # Search for the YAML file that might contain the definition
        components = name.split(".")
        sub = None
        prefix = None
        path = self.configdir
        for i, c in enumerate(components):
            path = path.joinpath(c)
            if path.with_suffix(".py").is_file():
                prefix = ".".join(components[:i+1])
                sub = ".".join(components[i+1:])
                path = path.with_suffix(path.suffix + ".py")
                break
            if not path.is_dir():
                logging.debug("Could not find %s", path)
                return None

        # Get the dataset
        logging.debug("Found file %s [prefix=%s/id=%s] in module %s", path, prefix, sub, self.module)
        module = importlib.import_module("%s.config.%s" % (self.module, prefix))
        for value in datasets(module):
            if name in value.__datamaestro__.aliases:
                return value.__datamaestro__

        return None

    def module(self, did):
        """Returns a module given the its id"""
        path = self.basedir.joinpath("config").joinpath(*did.split(".")).with_suffix(".py")
        return module.create(self, did, path)

    def modules(self):
        """Iterates over all modules in this repository"""
        for _, fid, package in self._modules():
            try:
                module = importlib.import_module(package)
                yield datasets(module)
            except Exception as e:
                import traceback
                traceback.print_exc()
                logging.error("Error while loading module %s: %s", package, e)

    def _modules(self):
        """Iterate over modules (without parsing them)"""
        import pkgutil

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
   
    def __iter__(self):
        """Iterates over all datasets in this repository"""
        for datasets in self.modules():
            for dataset in datasets:
                yield dataset.__datamaestro__


    @property
    def generatedpath(self):
        return self.basedir.joinpath("generated")

    @property
    def downloadpath(self):
        return self.context.datapath.joinpath(self.id)
        
    @property
    def datapath(self):
        return self.context.datapath.joinpath(self.id)

    @property
    def extrapath(self):
        """Path to the directory containing extra configuration files"""
        return self.basedir.joinpath("data")




def find_dataset(dataset_id: str):
    """Find a dataset given its id"""
    return DatasetDefinition.find(dataset_id)

def prepare_dataset(dataset_id: str):
    """Find a dataset given its id"""
    ds = DatasetDefinition.find(dataset_id) 
    return ds.prepare(download=True)

