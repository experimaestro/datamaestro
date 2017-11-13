"""
Contains 
"""

import logging
import os.path as op, os
import re
import yaml
import importlib
from pathlib import Path
import sys

YAML_SUFFIX = ".yaml"


class Importer(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith("datasets.r."):
            names = fullname.split(".")[2:]
            names.insert(1, "module")
            path = Path("/Users/bpiwowar/datasets/repositories").joinpath(*names)
            pypath = path.with_suffix(".py")
            if pypath.is_file():
                path = pypath
            else:
                path = path.joinpath("__init__.py")
                if not path.is_file():
                    logging.warn("Could not find %s", path)
                    return None
            loader = importlib.machinery.SourceFileLoader(fullname, str(path))

            spec = importlib.machinery.ModuleSpec(fullname, loader, is_package=True)
            return spec

        return None
sys.meta_path.append(Importer())


class Context(object):
    """Context of a configuration file"""
    def __init__(self, prefix):
        self.prefix = prefix
        
    def id(self, _id):
        return "%s.%s" % (self.prefix, _id)

class Repository:
    """A repository"""
    def __init__(self, config, basedir):
        self.basedir = basedir
        self.config = config
        self.etcdir = op.join(basedir, "etc")

    def __repr__(self):
        return "Repository(%s)" % self.basedir

    def search(self, name: str):
        """Search for a dataset in the definitions"""
        from .handlers.datasets import DataFile
        logging.debug("Searching for %s in %s", name, self.etcdir)
        components = name.split(".")
        sub = None
        prefix = None
        path = self.etcdir
        for i, c in enumerate(components):
            path = op.join(path, c)    
            if op.isfile(path + YAML_SUFFIX):
                prefix = ".".join(components[:i+1])
                sub = ".".join(components[i+1:])
                path += YAML_SUFFIX
                break
            if not op.isdir(path):
                logging.error("Could not find %s", path)
                return None

        # Get the dataset
        logging.debug("Found file %s [prefix=%s/id=%s]", path, prefix, sub)
        f = DataFile(self, prefix, path)
        if not sub in f:
            return None
        return f[sub]

    def __iter__(self):
        """Iterates over all datasets in this repository"""
        logging.debug("Looking at definitions in %s", self.etcdir)
        for root, dirs, files in os.walk(self.etcdir, topdown=False):
            relroot = Path(root).relative_to(self.etcdir)
            prefix = ".".join(relroot.parts)
            for relpath in files:
                try:
                    if relpath.endswith(YAML_SUFFIX):
                        path = op.join(root, relpath)
                        datafile = DataFile(self, "%s.%s" % (prefix, Path(relpath).stem), path)
                        for dataset in datafile:
                            yield dataset
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    logging.error("Error while reading definitions file %s: %s", relpath, e)
        
    def findhandler(self, handlertype, name):
        """
        Find a handler
        """
        logging.debug("Searching for handler %s of type %s", name, handlertype)
        pattern = re.compile(r"^(?:(/)|(?:(\w+):))?(?:([.\w]+)/)?(\w)(\w+)$")
        m = pattern.match(name)
        if not m:
            raise Exception("Invalid handler specification %s" % name)

        root = m.group(1)
        repo = m.group(2)
        name = m.group(4).upper() + m.group(5)
        if root:
            package = "datasets.handlers.%s" % (handlertype)
        elif repo:
            package = "datasets.r.%s.handlers.%s" % (repo, handlertype)
        else:
            package = "datasets.r.%s.handlers.%s" % (self.basedir.stem, handlertype)

        if m.group(3):
            package = "%s.%s" % (package, m.group(3))
        
        logging.debug("Searching for handler: package %s, class %s", package, name)
        package = importlib.import_module(package)

        return getattr(package, name)

    @property
    def generatedpath(self):
        return self.basedir.joinpath("generated")

    @property
    def downloadpath(self):
        return self.basedir.joinpath("downloads")

    @property
    def extrapath(self):
        """Path to the directory containing extra configuration files"""
        return self.basedir.joinpath("data")


class RegistryEntry:
    def __init__(self, registry, key):    
        self.key = key
        self.dicts = []
        _key = ""   
        for subkey in self.key.split("."):
            _key = "%s.%s" % (_key, subkey) if _key else subkey
            if _key in registry.content:
                self.dicts.insert(0, registry.content[_key])
        
    def __getitem__(self, key):
        for d in self.dicts:
            if key in d:
                return d[key]
        raise KeyError(key)


class Registry:
    def __init__(self, path):
        self.path = path
        if path.is_file():
            with open(path, "r") as fp:
                self.content = yaml.safe_load(fp)

    def __getitem__(self, key):
        return RegistryEntry(self, key)


class Compression:
    @staticmethod
    def extension(definition):
        if not definition: 
            return ""
        if definition == "gzip":
            return ".gz"

        raise Exception("Not handled compression definition: %s" % definition)


class Configuration:
    """
    Represents the configuration
    """
    MAINDIR = Path("~/datasets").expanduser()

    """Main settings"""
    def __init__(self, path: Path):
        self._path = path
        self.registry = Registry(self._path.joinpath("registry.yaml"))

    @property
    def repositoriespath(self):
        """Directory containing repositories"""
        return self._path.joinpath("repositories")

    @property
    def datapath(self):
        return self._path.joinpath("data")

    @property
    def datasetspath(self):
        return self._path.joinpath("datasets")

    @property
    def webpath(self) -> Path:
        return self._path.joinpath("www")


    def repositories(self):
        """Returns an iterator over definitions base directories"""
        yielded = False
        for name in os.listdir(self.repositoriespath):
            path = op.join(self.repositoriespath, name)
            if op.isdir(path):
                yield Repository(self, Path(path))

        if not yielded: return []

    def datasets(self):
        """Returns an iterator over all files"""
        for definitions in self.repositories():
            for dataset in definitions:
                yield dataset
    


class Data:
    def __init__(self, context, config):
        self.id = config.id
        if type(config.id) == list:
            self.aliases = self.id
            self.id = self.id[0]
        
class Documents:
    def __init__(self, context, config):
        pass

