from pathlib import Path
import yaml
import sys
import importlib
import os
import hashlib
import logging
import inspect
import urllib
import shutil
from .registry import Registry
from itertools import chain
import json
import pkg_resources
from tqdm import tqdm
from .utils import CachedFile
from typing import Iterable, List

class Compression:
    @staticmethod
    def extension(definition):
        if not definition: 
            return ""
        if definition == "gzip":
            return ".gz"

        raise Exception("Not handled compression definition: %s" % definition)



class DownloadReportHook(tqdm):
    """Report hook for tqdm when downloading from the Web"""
    def __init__(self, **kwargs):
        kwargs.setdefault("unit", "B")
        kwargs.setdefault("unit_scale", True)
        kwargs.setdefault("miniters", 1)
        super().__init__(**kwargs)
    def __call__(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize
        

def flatten_settings(settings, content, prefix=""):
    for key, value in content.items():
        key = "%s.%s" % (prefix, key) if prefix else key
        if isinstance(value, dict):
            flatten_settings(settings, value, key)
        else:
            settings[key] = value

class Context:
    """
    Represents the application context
    """
    MAINDIR = Path(os.environ.get("DATAMAESTRO_DIR", "~/datamaestro")).expanduser()

    INSTANCE=None

    """Main settings"""
    def __init__(self, path: Path = None):
        assert not Context.INSTANCE

        self._path = path or Context.MAINDIR
        self._dpath = Path(__file__).parents[1]
        self._repository = None
        self.registry = Registry(self.datapath / "registry.yaml")
        self.keep_downloads = False
        self.traceback = False

        # Read preferences
        self.settings = {}
        settingsPath = self._path / "settings.yaml"
        if settingsPath.is_file():
            with settingsPath.open("r") as fp:
                flatten_settings(self.settings, yaml.load(fp, Loader=yaml.SafeLoader))
                   
    @staticmethod
    def instance():
        if Context.INSTANCE is None:
            Context.INSTANCE = Context()
        return Context.INSTANCE
        
    @property
    def datapath(self):
        return self._path.joinpath("data")
        
    @property
    def cachepath(self) -> Path:
        return self._path.joinpath("cache")

    def repositories(self):
        """Returns an iterator over repositories"""
        for entry_point in pkg_resources.iter_entry_points('datamaestro.repositories'):
            yield entry_point.load().instance()

    def repository(self, repositoryid):
        l = [x for x in pkg_resources.iter_entry_points('datamaestro.repositories', repositoryid)]
        if not l:
            raise Exception("No datasets repository named %s", repositoryid)
        if len(l) > 1:
            raise Exception("Too many datasets repository named %s (%d)", repositoryid, len(l))
        return l[0].load()(self)

    def datasets(self):
        """Returns an iterator over all files"""
        for repository in self.repositories():
            for dataset in repository:
                yield dataset

    def dataset(self, datasetid) -> "datamaestro.definitions.DatasetDefinition":
        """Get a dataset by ID"""
        for repository in self.repositories():
            dataset = repository.search(datasetid)
            if dataset is not None:
                return dataset
        
        raise Exception("Dataset {} not found".format(datasetid))

    def preference(self, key, default=None):
        return self.settings.get(key, default)



    def downloadURL(self, url):
        """Downloads an URL"""

        self.cachepath.mkdir(exist_ok=True)

        def getPaths(hasher):
            """Returns a cache file path"""
            path = self.cachepath.joinpath(hasher.hexdigest())
            urlpath = path.with_suffix(".url")
            dlpath = path.with_suffix(".dl")
        
            if urlpath.is_file():
                if urlpath.read_text() != url:
                    # TODO: do something better
                    raise Exception("Cached URL hash does not match. Clear cache to resolve")
            return urlpath, dlpath

        hasher = hashlib.sha256(json.dumps(url).encode("utf-8"))

        urlpath, dlpath = getPaths(hasher)
        urlpath.write_text(url)

        if dlpath.is_file():
            logging.debug("Using cached file %s for %s", dlpath, url)
        else:

            logging.info("Downloading %s", url)
            tmppath = dlpath.with_suffix(".tmp")
            try:
                with DownloadReportHook(desc="Downloading %s" % url) as reporthook:
                    urllib.request.urlretrieve(url, tmppath, reporthook.__call__)
                shutil.move(tmppath, dlpath)
            except:
                tmppath.unlink()
                raise


        return CachedFile(dlpath, keep=self.keep_downloads)
        

class Datasets():
    def __init__(self, module):
        self.module = module

    @property
    def id(self):
        return ".".join(self.module.__name__.split(".", 2)[2:])

    @property
    def description(self):
        return self.module.__doc__ or ""

    def __iter__(self) -> Iterable["definitions.DatasetDefinition"]:
        for key, value in self.module.__dict__.items():
            # Ensures it is annotated
            if hasattr(value, "__datamaestro__"):
                # Ensures it is a dataset
                if value.__datamaestro__.aliases:
                    # Ensure it comes from the module
                    if self.module.__name__ == value.__datamaestro__.t.__module__:
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
        """Search for a dataset in the definitions
        """
        logging.debug("Searching for %s in %s", name, self.configdir)

        candidates: List[str] = []
        components = name.split(".")
        N = len(components)
        sub = None
        prefix = None
        path = self.configdir
        for i, c in enumerate(components):
            path = path / c

            if (path / "__init__.py").is_file():
                candidates.append(".".join(components[:i+1]))

            if path.with_suffix(".py").is_file():
                candidates.append(".".join(components[:i+1]))

            if not path.is_dir():
                break

        # Get the dataset
        for candidate in candidates[::-1]:
            logging.debug("Searching in module %s.config.%s", self.module, candidate)
            module = importlib.import_module("%s.config.%s" % (self.module, candidate))
            for value in Datasets(module):
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
                yield Datasets(module)
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
    def datapath(self):
        return self.context.datapath.joinpath(self.id)

    @property
    def extrapath(self):
        """Path to the directory containing extra configuration files"""
        return self.basedir.joinpath("data")




def find_dataset(dataset_id: str):
    """Find a dataset given its id"""
    from .definitions import DatasetDefinition
    return DatasetDefinition.find(dataset_id)

def prepare_dataset(dataset_id: str):
    """Find a dataset given its id"""
    from .definitions import DatasetDefinition
    ds = DatasetDefinition.find(dataset_id) 
    return ds.prepare(download=True)
