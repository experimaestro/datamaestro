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
import yaml
from typing import Union

from .context import Context, DownloadReportHook
from .utils import CachedFile

YAML_SUFFIX = ".yaml"

class DatasetReference:
    def __init__(self, value):
        self.value = value

    def resolve(self, reference):
        did = self.value
        logging.debug("Resolving dataset reference %s", did)

        pos = did.find("!")
        if pos > 0:
            namespace = did[:pos]
            name = did[pos+1:]
            did = "%s.%s" % (reference.resolvens(namespace), name)
        elif did.startswith("."):
            did = reference.baseid + did

        return DatasetDefinition.find(did, context=reference.context)

def datasetref(loader, node):
    """A dataset reference"""
    assert(isinstance(node.value, str))
    return DatasetReference(node.value)


def handlertag(loader: yaml.Loader, tag_suffix, node):
    """A handler tag"""
    v = loader.construct_mapping(node)
    v["__handler__"] = tag_suffix
    return v

class YAMLLoader(yaml.FullLoader):
    pass

YAMLLoader.add_constructor('!dataset', datasetref)
YAMLLoader.add_multi_constructor('!@', handlertag)

def readyaml(path):
    with open(path) as f:
        return yaml.load(f, Loader=YAMLLoader)

def readyamls(path):
    with open(path) as f:
        for p in yaml.load_all(f, Loader=YAMLLoader):
            yield p


class DataFile:
    """A data configuration file"""

    @staticmethod
    @lru_cache()
    def create(repository, prefix: str, path: str):
        return DataFile(repository, prefix, path)

    def __init__(self, repository, prefix: str, path: str):
        self.repository = repository
        logging.debug("Reading %s", path)
        self.path = path
        self.datasets = {}
        self.id = prefix

        self.main = None

        if path is not None:
            for doc in readyamls(path):
                fulldid = "%s.%s" % (prefix, doc["id"])  if "id" in doc else self.id
                ds = DatasetDefinition(self, fulldid, doc, self.main)
                self.datasets[fulldid] = ds

                if not self.main:
                    self.main = ds
                    self.name = doc.get("name", self.id)


    def __contains__(self, name):
        """Returns true if the dataset belongs to this datafile"""
        return name in self.datasets

    def __getitem__(self, name):
        return self.datasets[name]

    def resolvens(self, ns):
        return self.main["namespaces"][ns]

    def __len__(self):
        return len(self.datasets)

    def __iter__(self):
        return self.datasets.values().__iter__()

    @property
    def description(self):
        return self.main.get("description", "")

    @property
    def context(self):
        return self.repository.context

    @property
    def baseid(self):
        return self.id


class DatasetDefinition:
    """Represents one dataset definition"""

    def __init__(self, datafile: DataFile, datasetid: str, content: object, parent: "DatasetDefinition"):
        """
        Construct a new dataset

        :param datafile: the attached definition file
        :param id: the ID of this dataset
        :param content: The dataset definition
        """
        self.datafile = datafile
        self.id = datasetid
        self.content = content
        self.parent = parent
        self.isalias = isinstance(content, DatasetReference)

        # Don't define any dataset object for now
        self._dataset = None
        self._resolved = False
        
        # Search for dependencies
        self.dependencies = {}

        # Get some useful values from the definition
        self.type = self.content.get("type", None)
        self.version = self.content.get("version", None)
        self.name = self.content.get("name", self.id)



    def _resolve(self, path, content):
        """
        Resolve all dataset references

        Returns the content 
        """
        
        prefix = path + "." if path else ""

        if isinstance(content, dict):
            return {str(k): self._resolve(prefix + str(k), v) for k, v in content.items()}
        elif isinstance(content, list):
            return [self._resolve("%s.%d" % (prefix, i), v) for i, v in enumerate(content)]
        elif isinstance(content, DatasetReference):
            dataset = content.resolve(self)
            self.dependencies[path] = dataset
            return dataset
        return content

    def resolve(self):
        """Resolve all references within content"""
        if not self._resolved:
            self.content = self._resolve("", self.content)
            self._resolved = True


    @property
    def downloadHandler(self):
        from datamaestro.download import DownloadHandler
        return DownloadHandler.find(self, self.content["download"])

    @property
    def dataset(self):
        if not self._dataset:
            # Resolve the references
            self.resolve()
            
            if self.type:
                logging.debug("Searching for dataset object of type %s", self.type)
                self._dataset = self.repository.findhandler("dataset", self.type)()
            else:
                from datamaestro.dataset import Dataset
                self._dataset = Dataset()

        return self._dataset


    def download(self, force=False):
        """Download the dataset files
        
        Keyword Arguments:
            force {bool} -- Force the download of resources, even if already done (default: {False})
        
        Returns:
            A boolean indicating whether the download was succesful or not
        """
        logging.debug("Asked to download files for dataset %s", self.name)
        self.resolve()

        # (1) Download direct resources
        if "download" in self.content:
            handler = self.downloadHandler
            destpath = handler.path(self.destpath)
            handler.download(destpath)

        # (2) Download dependencies
        success = True
        for dependency in self.dependencies.values():
            logging.debug("Downloading dependency %s", dependency)
            success &= dependency.download()

        return success

    def prepare(self, download=False):
        """Prepare the dataset
        
        Performs (basic) post-processing after the dataset has been downloaded,
        and returns a Data object
        """
        self.resolve()

        if download:
            self.download()

        # Set some values
        self.dataset.id = self.id
        
        # Update all dependencies
        for key, dependency in self.dependencies.items():
            dependency.prepare(download=download)

        # Use the "files" section 
        if "files" in self.content:
            files = self.dataset.files
            for key, definition in self.content["files"].items():
                if isinstance(definition, str):
                    files[key] = self.destpath / definition
                elif isinstance(definition, DatasetDefinition):
                    # This is a dataset
                    files[key] = definition.prepare().files
                else:
                    filetype = definition.get("__handler__", None)
                    path = self.destpath / definition["path"]
                    if filetype:
                        files[key] = self.repository.findhandler_of("files", filetype)(path, filetype)
                    else:
                        files[key] = path

        # If not, use the download handler directly
        elif "download" in self.content:
            handler = self.downloadHandler
            self.dataset.files = handler.files(self.destpath)

        return self.dataset


    def description(self):
        """Returns the description of the dataset"""
        return self.content.get("description", "No description")

    def tags(self):
        """Returns the description of the dataset"""
        return self.content.get("tags", [])
        
    def tasks(self):
        """Returns the description of the dataset"""
        return self.content.get("tasks", [])


    @property
    def path(self) -> Path:
        path = Path(*self.id.split("."))
        if self.version:
            path = path.with_suffix(".v%s" % self.version)
        return path

    @property
    def extrapath(self):
        """Returns the path containing extra configuration files"""
        return self.repository.extrapath.joinpath(self.path)

    @property
    def destpath(self):
        """Returns the destination path for downloads"""
        return self.repository.downloadpath.joinpath(self.path)

    @property
    def generatedpath(self):
        """Returns the destination path for generated files"""
        return self.repository.generatedpath.joinpath(self.path)


    def parent(self):
        pos = self.id.rfind(".")
        return self.datafile.repository.search(self.id[:pos])

    @property
    def context(self):
        """Returns the context"""
        return self.datafile.context

    @property
    def ids(self):
        """Returns all the IDs of this dataset"""
        return [self.id]
    
    @property
    def repository(self) -> "Repository":
        """Main ID is the first one"""
        return self.datafile.repository

    @property
    def baseid(self):
        """Main ID is the first one"""
        return self.datafile.id

    def resolvens(self, ns):
        return self.datafile.resolvens(ns)

    def __repr__(self):
        return "DatasetDefinition(%s)" % (", ".join(self.ids))


    def __getitem__(self, key):
        """Get the item"""

        # If content is a dataset reference, then resolve it
        if isinstance(self.content, DatasetReference):
            self.content = self.content.resolve(self).content

        # Tries first ourselves, then go upward
        if key in self.content:
            return self.content[key]
        if self.parent:
            return self.parent[key]

        raise IndexError()
    

    def get(self, key, defaultValue):
        try:
            return self[key]
        except IndexError:
            return defaultValue

    @property
    def datadir(self):
        """Path containing real data"""
        datapath = self.datafile.repository.datapath
        if "datapath" in self:
            steps = self.id.split(".")
            steps.extend(self["datapath"].split("/"))
        else:
            steps = self.id.split(".")
        return datapath.joinpath(*steps)

    @property
    def description(self):
        return self.handler.description()

    @property
    def tags(self):
        return self.handler.tags()

    @property
    def tasks(self):
        return self.handler.tasks()

    def downloadURL(self, url):
        """Downloads an URL"""

        self.context.cachepath.mkdir(exist_ok=True)

        def getPaths(hasher):
            """Returns a cache file path"""
            path = self.context.cachepath.joinpath(hasher.hexdigest())
            urlpath = path.with_suffix(".url")
            dlpath = path.with_suffix(".dl")
        
            if urlpath.is_file():
                if urlpath.read_text() != url:
                    # TODO: do something better
                    raise Exception("Cached URL hash does not match. Clear cache to resolve")
            return urlpath, dlpath

        hasher = hashlib.sha256(json.dumps(url).encode("utf-8"))

        if isinstance(url, dict):
            logging.info("Needs to download file %s", url["name"])
            handler = self.repository.findhandler("download", url["handler"])(self, url)
            urlpath, dlpath = getPaths(hasher)
            handler.download(dlpath)
            return CachedFile(dlpath, keep=self.context.keep_downloads)


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


        return CachedFile(dlpath, keep=self.context.keep_downloads)
        
        
    @staticmethod
    def find(name: str, *, context: "Context" = Context.default_context()) -> "DatasetDefinition":
        """Find a dataset given its name"""
        logging.debug("Searching dataset %s", name)
        for repository in context.repositories():
            logging.debug("Searching dataset %s in %s", name, repository)
            dataset = repository.search(name)
            if dataset is not None:
                return dataset
        raise Exception("Could not find the dataset %s" % (name))


def find_dataset(dataset_id: str):
    """Find a dataset given its id"""
    return DatasetDefinition.find(dataset_id)

def prepare_dataset(dataset_id: str, context=Context.default_context()):
    """Find a dataset given its id"""
    ds = DatasetDefinition.find(dataset_id, context=context)
    return ds.prepare(download=True)


class Repository:
    """A repository regroup a set of datasets and their corresponding specific handlers (downloading, filtering, etc.)"""

    def __init__(self, context: Context, basedir:Path= None):
        """Initialize a new repository

        :param context: The dataset main context
        :param basedir: The base directory of the repository
            (by default, the same as the repository class)
        """
        self.context = context
        self.basedir = basedir 
        if not self.basedir:
            p = inspect.getabsfile(self.__class__)
            self.basedir = Path(p).parent
        self.configdir = self.basedir.joinpath("config")
        self.id = self.__class__.NAMESPACE
        self.name = self.id
        self.module = self.__class__.__module__
        
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
            if path.with_suffix(YAML_SUFFIX).is_file():
                prefix = ".".join(components[:i+1])
                sub = ".".join(components[i+1:])
                path = path.with_suffix(path.suffix + YAML_SUFFIX)
                break
            if not path.is_dir():
                logging.debug("Could not find %s", path)
                return None

        # Get the dataset
        logging.debug("Found file %s [prefix=%s/id=%s]", path, prefix, sub)
        f = DataFile.create(self, prefix, path)
        if not name in f:
            return None

        dataset = f[name]
        if isinstance(dataset.content, DatasetReference):
            dataset = dataset.content.resolve(f)
        return dataset

    def datafile(self, did):
        """Returns a datafile given the its id"""
        path = self.basedir.joinpath("config").joinpath(*did.split(".")).with_suffix(YAML_SUFFIX)
        return DataFile.create(self, did, path)

    def datafiles(self):
        """Iterates over all datafiles in this repository"""
        logging.debug("Looking at definitions in %s", self.configdir)
        for path in self.configdir.rglob("*%s" % YAML_SUFFIX):
            try:
                c = [p.name for p in path.relative_to(self.configdir).parents][:-1][::-1]
                c.append(path.stem)
                fid = ".".join(c)
                datafile = DataFile.create(self, fid, path)
                yield datafile
            except Exception as e:
                import traceback
                traceback.print_exc()
                logging.error("Error while reading definitions file %s: %s", path, e)

    def __iter__(self):
        """Iterates over all datasets in this repository"""
        for datafile in self.datafiles():
            for dataset in datafile:
                yield dataset

    def findhandler_of(self, handlertype: str, handlerdef: Union[dict, str]) -> "FileHandler":
        """Returns the handler of a given type
        
        Arguments:
            handlertype {str} -- The handler type
            handlerdef {Union[dict, str]} -- Either a structure containing "__handler__" or a string
        
        Returns:
            handler -- Returns the handler
        """
        if isinstance(handlerdef, str):
            return self.findhandler(handlertype, handlerdef)
        return self.findhandler(handlertype, handlerdef["__handler__"])

    def findhandler(self, handlertype, fullname):
        """
        Find a handler of a given type

        A handle can be specified using

        `module/subpackage:class`

        will map to class <class> in <module>.<handlertype>.subpackage

        Two shortcuts can be used:
        - `/subpackage:class`: module = datamaestro
        - `subpackage:class`: module = repository module
        
        If `class` is not given, use the last subpackage name (with a uppercase first letter)

        """
        logging.debug("Searching for handler %s of type %s", fullname, handlertype)
        pattern = re.compile(r"^((?P<module>[\w_]+)?(?P<slash>/))?(?P<path>[\w_]+)(?::(?P<name>[\w_]+))?$")
        m = pattern.match(fullname)
        if not m:
            raise Exception("Invalid handler specification %s" % fullname)

        name = m.group('name')
        path = m.group('path')
        if name is None:
            mpath = re.match(r"^(?:.*\.)?([^.])([^.]+)$", path)
            name = mpath.group(1).upper() + mpath.group(2)
            
        if m.group('slash') is None:
            # relative path
            module = self.module
        else:
            # absolute path
            if m.group('module'):
                module = m.group('module')
            else:
                module = "datamaestro"
        

        package = "%s.%s.%s" % (module, handlertype, path)
        
        logging.debug("Searching for handler: package %s, class %s", package, name)
        try:
            package = importlib.import_module(package)
        except ModuleNotFoundError:
            raise Exception("""Could not find handler "{}" of type {}: module {} not found""".format(fullname, handlertype, package))

        return getattr(package, name)

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
