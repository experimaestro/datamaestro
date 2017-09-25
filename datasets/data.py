import logging
import os.path as op, os
import yaml
import importlib

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



class Context(object):
    """Context of a configuration file"""
    def __init__(self, prefix):
        self.prefix = prefix
        
    def id(self, _id):
        return "%s.%s" % (self.prefix, _id)

class Configuration:
    """Main settings"""
    def __init__(self, path):
        self._path = path

    @property
    def configpath(self):
        return op.join(self._path, "config")

    @property
    def datapath(self):
        return op.join(self._path, "data")

    @property
    def datasetspath(self):
        return op.join(self._path, "datasets")

    @property
    def webpath(self):
        return op.join(op.dirname(self._path), "www")


class Data:
    def __init__(self, context, config):
        self.id = config.id
        if type(config.id) == list:
            self.aliases = self.id
            self.id = self.id[0]
        
class Documents:
    def __init__(self, context, config):
        pass
        


class Dataset:
    def __init__(self, id, content):
        self.id = id
        self.content = content

    def getHandler(self, config):
        name = self.content["handler"]
        logging.debug("Searching for handler %s", name)
        package, name = name.split("/")
        name = name[0].upper() + name[1:]
        
        package = importlib.import_module("datasets.handlers." + package, package="")
        return getattr(package, name)(config, self.id, self.content)

    @staticmethod
    def find(configuration, name):
        """Find a dataset given its name"""
        logging.debug("Searching dataset %s" % name)
        path = configuration.configpath
        components = name.split(".")
        sub = None
        prefix = None
        for i, c in enumerate(components):
            path = op.join(path, c)    
            if op.isfile(path + YAML_SUFFIX):
                prefix = ".".join(components[:i+1])
                sub = ".".join(components[i+1:])
                path += YAML_SUFFIX
                break
            if not op.isdir(path):
                raise OSError("Path {} does not exist".format(path))

        # Get the dataset
        logging.debug("Found file %s [prefix=%s/id=%s]", path, prefix, sub)
        # TODO: cache datafile
        f = DataFile(prefix, path)
        if not sub in f:
            raise Exception("Could not find the dataset %s in %s" % (name, path))
        return f[sub]
        


class Handler:
    def __init__(self, config, id, content):
        self.config = config
        self.id = id
        self.content = content

        # Search for dependencies
        self.dependencies = {}
        self._searchdependencies(config, self.content)

    def _searchdependencies(self, config, content):
        if isinstance(content, dict):
            for k, v in content.items():
                self._searchdependencies(config, v)
        elif isinstance(content, list):
            for v in content:
                self._searchdependencies(config, v)
        elif isinstance(content, DatasetReference):
            did = content.value
            if did.startswith("."):
                did = self.id[:self.id.rfind(".")] + did


            dataset = Dataset.find(config, did)
            handler = dataset.getHandler(config)
            self.dependencies[did] = handler

    """Base class for all handlers"""
    def download(self, force=False):
        logging.info("Downloading %s", self.content["description"])

        # Download direct resources
        if "download" in self.content:
            handler = DownloadHandler.find(self.content["download"])
            if op.exists(self.destpath) and not force:
                logging.info("File already downloaded")
            else:
                handler.download(self.destpath)

        # Download dependencies
        for dependency in self.dependencies.values():
            dependency.download()

    def description(self):
        return self.content["description"]

    @property
    def destpath(self):
        return op.join(self.config.datasetspath, *self.id.split("."))



class DownloadHandler:
    def __init__(self, definition):
        self.definition = definition

    @staticmethod
    def find(definition):
        name = definition["handler"]
        logging.debug("Searching for download handler %s", name)
        package, name = name.split("/")
        name = name[0].upper() + name[1:]
        
        package = importlib.import_module("datasets.download." + package, package="")
        return getattr(package, name)(definition)



class DataFile:
    def __init__(self, prefix, path):
        self.content = readyaml(path)
        self.datasets = {}
        for d in self.content["data"]:
            if type(d["id"]) == list:
                for _id in d["id"]:
                    self.datasets[_id] = Dataset("%s.%s" % (prefix, _id), d)
            else:
                self.datasets[d["id"]] = Dataset("%s.%s" % (prefix, d["id"]), d)

        logging.debug("Found datasets: %s", ", ".join(self.datasets.keys()))

    def __contains__(self, name):
        return name in self.datasets

    def __getitem__(self, name):
        return self.datasets[name]
