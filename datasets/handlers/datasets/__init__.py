"""
Datasets handler
"""

import logging
import yaml
import os.path as op
from datasets.context import Context
from datasets.data import Dataset, DatasetReference, Repository

class DatasetHandler:
    """Base class for all dataset handlers"""
    def __init__(self, dataset: Dataset, content, handleroptions):
        self.context = dataset.repository.context
        self.dataset = dataset
        self.content = content

        # Search for dependencies
        self.dependencies = {}
        self.content = self._resolve(self.context, "", self.content)

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
            dataset = content.resolve(self.dataset)
            self.dependencies[path] = dataset
            return dataset
        
        return content

    def download(self, force=False):
        """Download all the resources (if available)"""
        from datasets.handlers.download import DownloadHandler
        logging.info("Downloading %s", self.content.get("description", self.dataset))

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
            logging.debug("Downloading dependency %s", dependency)
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
