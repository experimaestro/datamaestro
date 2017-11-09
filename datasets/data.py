import logging
import os.path as op, os
import yaml
import importlib
from pathlib import Path
import re

YAML_SUFFIX = ".yaml"


class DatasetReference:
    def __init__(self, value):
        self.value = value

def datasetref(loader, node):
    assert(type(node.value) == str)
    return DatasetReference(node.value)

yaml.SafeLoader.add_constructor('!dataset', datasetref)

def readyaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


def findhandler(handlertype, name, definition):
    logging.debug("Searching for %s handler %s", handlertype, name)
    l = name.split("/")
    if len(l) == 2:
        package, name = name.split("/")
        package = importlib.import_module("datasets.%s.%s" % (handlertype, package), package="")
    else:
        package = importlib.import_module("datasets.%s.main" % (handlertype), package="")
        name = l[0]

    name = name[0].upper() + name[1:]
    return getattr(package, name)(definition)

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


class Configuration:
    """
    Represents the configuration
    """
    MAINDIR = Path("~/datasets").expanduser()

    """Main settings"""
    def __init__(self, path: Path):
        self._path = path

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

    def finddataset(self, name):
        return Dataset.find(self, name)

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


class DataFile:
    """A single dataset definition file"""
    def __init__(self, repository: Repository, prefix: str, path: str):
        self.repository = repository
        logging.debug("Reading %s", path)
        self.content = readyaml(path)
        if not self.content: self.content = {}
        self.datasets = {}
        self.id = prefix

        for d in self.content.get("data", []):
            if "id" not in d:
                self.datasets[prefix] = Dataset(self, [prefix], d)
            elif type(d["id"]) == list:
                ids = ["%s.%s" % (prefix, d["id"][0]) for _id in d["id"]]
                dataset = Dataset(self, ids, d)
                for _id in d["id"]:
                    self.datasets[_id] = dataset
            else:
                self.datasets[d["id"]] = Dataset(self, ["%s.%s" % (prefix, d["id"])], d)

    def __contains__(self, name):
        return name in self.datasets

    def __getitem__(self, name):
        return self.datasets[name]

    def resolvens(self, ns):
        return self.content["namespaces"][ns]

    def __iter__(self):
        return self.datasets.values().__iter__()
    
    @property
    def downloadpath(self):
        return op.join(self.repository.basedir, "downloads")
        


class Dataset:
    """Represents one dataset"""

    def __init__(self, datafile: DataFile, ids, content):
        """
        Construct a new dataset

        :param datafile: the attached definition file
        :param ids: the IDs of this dataset
        :param content: The dataset definition
        """
        self.datafile = datafile
        self.ids = ids
        self.content = content

    @property
    def id(self):
        """Main ID is the first one"""
        return self.ids[0]

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

    def getHandler(self):
        name = self.content["handler"]
        logging.debug("Searching for handler %s", name)

        pattern = re.compile(r"(/|\w+:)?(\w+)/(\w\w+)")
        m = pattern.match(name)
        print(m)
        package, name = name.split("/")
        name = name[0].upper() + name[1:]
        try:
            # Try in repository
            path = self.repository.basedir.joinpath("module")
            pname = "handlers.%s" % package
            print(pname, importlib.util.find_spec(pname, [path]))
            print()
        except ModuleNotFoundError:
            # Try main code source
            package = importlib.import_module("datasets.handlers." + package, package="")
    
        return getattr(package, name)(self.datafile.repository.config, self, self.content)

    @property
    def downloadpath(self):
        return self.datafile.downloadpath

    @staticmethod
    def find(config: Configuration, name: str):
        """Find a dataset given its name"""
        logging.debug("Searching dataset %s", name)
        for repository in config.repositories():
            logging.debug("Searching dataset %s in %s", name, repository)
            dataset = repository.search(name)
            if dataset is not None:
                return dataset
        raise Exception("Could not find the dataset %s" % (name))
        


class Handler:
    """Base class for all dataset handlers"""
    def __init__(self, config, dataset: Dataset, content):
        self.config = config
        self.dataset = dataset
        self.content = content

        # Search for dependencies
        self.dependencies = {}
        self._searchdependencies(config, self.content)


    def _searchdependencies(self, config, content):
        """Retrieve all dependencies"""
        if isinstance(content, dict):
            for k, v in content.items():
                self._searchdependencies(config, v)
        elif isinstance(content, list):
            for v in content:
                self._searchdependencies(config, v)
        elif isinstance(content, DatasetReference):
            did = content.value
            pos = did.find("!")
            if pos > 0:
                namespace = did[:pos]
                name = did[pos+1:]
                did = "%s.%s" % (self.dataset.resolvens(namespace), name)
            elif did.startswith("."):
                did = self.dataset.baseid + did

            dataset = Dataset.find(config, did)
            handler = dataset.getHandler()
            self.dependencies[did] = handler

    def download(self, force=False):
        """Download all the resources (if available)"""
        logging.info("Downloading %s", self.content["description"])

        # Download direct resources
        if "download" in self.content:
            handler = DownloadHandler.find(self.content["download"])
            if op.exists(self.destpath) and not force:
                logging.info("File already downloaded [%s]", self.destpath)
            else:
                handler.download(self.destpath)

        # Download dependencies
        for dependency in self.dependencies.values():
            dependency.download()

    def prepare(self, **kwargs):
        """Prepare the dataset"""
        pass 

    def description(self):
        return self.content["description"]

    @property
    def destpath(self):
        return op.join(self.dataset.downloadpath, *self.dataset.id.split("."))


class DownloadHandler:
    def __init__(self, definition):
        self.definition = definition

    @staticmethod
    def find(definition):
        return findhandler("download", definition["handler"], definition)


