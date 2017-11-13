"""
Datasets handler
"""

import logging
import yaml
import os.path as op
from datasets.data import Repository, Configuration

class DatasetReference:
    def __init__(self, value):
        self.value = value

def datasetref(loader, node):
    """A dataset reference"""
    assert(isinstance(node.value, str))
    return DatasetReference(node.value)

yaml.SafeLoader.add_constructor('!dataset', datasetref)

def readyaml(path):
    with open(path) as f:
        return yaml.safe_load(f)



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
        return self.repository.findhandler("dataset", name)(self, self.content)


    def __getitem__(self, key):
        if key in self.content:
            return self.content[key]

        return self.datafile.content[key]

    @property
    def datadir(self):
        """Path containing real data"""
        datapath = self.datafile.repository.config.datapath
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
    def find(config: Configuration, name: str):
        """Find a dataset given its name"""
        logging.debug("Searching dataset %s", name)
        for repository in config.repositories():
            logging.debug("Searching dataset %s in %s", name, repository)
            dataset = repository.search(name)
            if dataset is not None:
                return dataset
        raise Exception("Could not find the dataset %s" % (name))
        

class DatasetHandler:
    """Base class for all dataset handlers"""
    def __init__(self, dataset: Dataset, content):
        self.config = dataset.repository.config
        self.dataset = dataset
        self.content = content

        # Search for dependencies
        self.dependencies = {}
        self.content = self._resolve(self.config, "", self.content)

    def _resolve(self, config, path, content):
        """
        Resolve all dependent datasets by finding appropriate handlers

        Returns the content 
        """
        
        prefix = path + "." if path else ""

        if isinstance(content, dict):
            return {k: self._resolve(config, prefix + k, v) for k, v in content.items()}

        elif isinstance(content, list):
            return [self._resolve(config, "%s.%d" % (prefix, i), v) for i, v in enumerate(content)]
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
            self.dependencies[path] = handler
            return handler
        
        return content

    def download(self, force=False):
        """Download all the resources (if available)"""
        from datasets.handlers.download import DownloadHandler
        logging.info("Downloading %s", self.content["description"])

        # Download direct resources
        if "download" in self.content:
            handler = DownloadHandler.find(self.repository, self.content["download"])
            if op.exists(self.destpath) and not force:
                logging.info("File already downloaded [%s]", self.destpath)
            else:
                handler.download(self.destpath)

        # Download dependencies
        success = True
        for dependency in self.dependencies.values():
            success &= dependency.download()

        return success

    def prepare(self):
        """Prepare the dataset"""
        p = {}
        for key, dependency in self.dependencies.items():
            p[key] = dependency.prepare()

        return p

    def description(self):
        """Returns the description of the dataset"""
        return self.content["description"]

    @property
    def repository(self):
        return self.dataset.repository

    @property
    def extrapath(self):
        """Returns the path containing extra configuration files"""
        return self.repository.extrapath.joinpath(*self.dataset.id.split("."))

    @property
    def destpath(self):
        """Returns the destination path for downloads"""
        return self.repository.downloadpath.joinpath(*self.dataset.id.split("."))

    @property
    def generatedpath(self):
        """Returns the destination path for downloads"""
        return self.repository.generatedpath.joinpath(*self.dataset.id.split("."))
