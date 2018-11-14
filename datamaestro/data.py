"""
Contains 
"""

import sys
import os
import tempfile
import urllib
import shutil
import logging
import re
import inspect
from .context import Context
import urllib.request
from pathlib import Path
from itertools import chain
import importlib

import yaml

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

        return Dataset.find(reference.context, did)

def datasetref(loader, node):
    """A dataset reference"""
    assert(isinstance(node.value, str))
    return DatasetReference(node.value)

yaml.Loader.add_constructor('!dataset', datasetref)

def readyaml(path):
    with open(path) as f:
        return yaml.load(f)



class DataFile:
    """A single dataset definition file"""
    def __init__(self, repository, prefix: str, path: str):
        self.repository = repository
        logging.debug("Reading %s", path)
        self.path = path
        self.content = readyaml(path)
        self.content = self.content or {}
        self.datasets = {}
        self.id = prefix

        # A dataset can either be nested within data, or not
        data = self.content.get("data", {None: self.content})
        for did, d in data.items():
            fulldid = "%s.%s" % (prefix, did) if did else prefix
            self.datasets[fulldid] = Dataset(self, fulldid, d)

    def __contains__(self, name):
        return name in self.datasets

    def __getitem__(self, name):
        return self.datasets[name]

    def resolvens(self, ns):
        return self.content["namespaces"][ns]

    def __iter__(self):
        return self.datasets.values().__iter__()

    @property
    def description(self):
        return self.content.get("description", "")
    @property
    def context(self):
        return self.repository.context

    @property
    def baseid(self):
        return self.id
    

class Dataset:
    """Represents one dataset"""

    def __init__(self, datafile: DataFile, datasetid: str, content):
        """
        Construct a new dataset

        :param datafile: the attached definition file
        :param id: the ID of this dataset
        :param content: The dataset definition
        """
        self.datafile = datafile
        self.id = datasetid
        self.content = content
        self._handler = None
        self.isalias = isinstance(content, DatasetReference)

    @property
    def context(self):
        """Returns the context"""
        return self.datafile.context

    @property
    def ids(self):
        """Returns all the IDs of this dataset"""
        return [self.id]
    
    @property
    def repository(self):
        """Main ID is the first one"""
        return self.datafile.repository

    @property
    def baseid(self):
        """Main ID is the first one"""
        return self.datafile.id

    def resolvens(self, ns):
        return self.datafile.resolvens(ns)

    def __repr__(self):
        return "Dataset(%s)" % (", ".join(self.ids))

    def __contains__(self, key):
        if isinstance(self.content, DatasetReference):
            return key in self.content.resolve(self).content
        if key in self.content:
            return True
        return key in self.datafile.content

    

    def __getitem__(self, key):
        if isinstance(self.content, DatasetReference):
            self.content = self.content.resolve(self).content

        if key in self.content:
            return self.content[key]

        return self.datafile.content[key]

    

    def get(self, key, defaultvalue):
        if isinstance(self.content, DatasetReference):
            self.content = self.content.resolve(self).content
            
        if key in self.content:
            return self.content.get(key, defaultvalue)

        return self.datafile.content.get(key, defaultvalue)

    @property
    def datadir(self):
        """Path containing real data"""
        datapath = self.datafile.repository.context.datapath
        if "datapath" in self.content:
            steps = self.id.split(".")
            steps.extend(self.content["datapath"].split("/"))
        elif "datapath" in self.datafile.content:
            steps = self.datafile.id.split(".")
            steps.extend(self.datafile.content["datapath"].split("/"))
        else:
            steps = self.id.split(".")
        return datapath.joinpath(*steps)

    @staticmethod
    def find(config: "Context", name: str):
        """Find a dataset given its name"""
        logging.debug("Searching dataset %s", name)
        for repository in config.repositories():
            logging.debug("Searching dataset %s in %s", name, repository)
            dataset = repository.search(name)
            if dataset is not None:
                return dataset
        raise Exception("Could not find the dataset %s" % (name))

    @property
    def handler(self):
        if not self._handler:
            if "handler" in self:
                name = self["handler"]
                if isinstance(name, dict):
                    (key, value), = name.items()
                    self._handler = self.repository.findhandler("dataset", key)(self, self.content, value)                
                else:
                    self._handler = self.repository.findhandler("dataset", name)(self, self.content, None)
            else:
                from datamaestro.handlers.dataset import DatasetHandler
                self._handler = DatasetHandler(self, self.content, None)
        return self._handler

    def download(self):
        return self.handler.download()

    def description(self):
        return self.handler.description()

    def tags(self):
        return self.handler.tags()

    def prepare(self):
        return self.handler.prepare()
        

class Repository:
    """A repository"""
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
        
    def __repr__(self):
        return "Repository(%s)" % self.basedir

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
                logging.error("Could not find %s", path)
                return None

        # Get the dataset
        logging.debug("Found file %s [prefix=%s/id=%s]", path, prefix, sub)
        f = DataFile(self, prefix, path)
        if not name in f:
            return None

        dataset = f[name]
        if isinstance(dataset.content, DatasetReference):
            dataset = dataset.content.resolve(f)
        return dataset

    def datafile(self, did):
        """Returns a datafile given the its id"""
        path = self.basedir.joinpath("etc").joinpath(*did.split(".")).with_suffix(YAML_SUFFIX)
        return DataFile(self, did, path)

    def datafiles(self):
        """Iterates over all datafiles in this repository"""
        logging.debug("Looking at definitions in %s", self.configdir)
        for path in self.configdir.rglob("*%s" % YAML_SUFFIX):
            try:
                c = [p.name for p in path.relative_to(self.configdir).parents][:-1][::-1]
                c.append(path.stem)
                fid = ".".join(c)
                datafile = DataFile(self, fid, path)
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

    def findhandler(self, handlertype, fullname):
        """
        Find a handler of a given type
        """
        logging.debug("Searching for handler %s of type %s", fullname, handlertype)
        pattern = re.compile(r"^(?:(/)|(?:(\w+):))?(?:([.\w]+)/)?(\w)(\w+)$")
        m = pattern.match(fullname)
        if not m:
            raise Exception("Invalid handler specification %s" % name)

        root = m.group(1)
        repo = m.group(2)
        name = m.group(4).upper() + m.group(5)
        # if root:
        package = "datamaestro.handlers.%s" % (handlertype)
        # elif repo:
        #     package = "datamaestro.r.%s.handlers.%s" % (repo, handlertype)
        # else:
        #     package = "datamaestro.r.%s.handlers.%s" % (self.basedir.stem, handlertype)

        if m.group(3):
            package = "%s.%s" % (package, m.group(3))
        
        logging.debug("Searching for handler: package %s, class %s", package, name)
        try:
            package = importlib.import_module(package)
        except ModuleNotFoundError:
            raise Exception(f"""Could not find handler "{fullname}" of type {handlertype}: module {package} not found""")

        return getattr(package, name)

    @property
    def generatedpath(self):
        return self.basedir.joinpath("generated")

    @property
    def downloadpath(self):
        return self.context.datapath.joinpath(self.id)

    @property
    def extrapath(self):
        """Path to the directory containing extra configuration files"""
        return self.basedir.joinpath("data")

