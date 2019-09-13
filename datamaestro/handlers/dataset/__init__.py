"""
Datasets handler
"""

import logging
import yaml
from pathlib import Path
from datamaestro.context import Context
from datamaestro.data import Dataset, DatasetReference, Repository
from datamaestro.handlers.download import DownloadHandler

class DatasetHandler:
    """Base class for all dataset handlers"""
    def __init__(self, dataset: Dataset, content, handleroptions):
        self.context = dataset.repository.context
        self.dataset = dataset

        # Search for dependencies
        self.dependencies = {}
        self.content = self._resolve(self.context, "", content)
        self.version = self.content.get("version", None)

    def _resolve(self, config, path, content):
        """
        Resolve all dependent datasets by finding appropriate handlers

        Returns the content 
        """
        
        prefix = path + "." if path else ""

        if isinstance(content, dict):
            return {str(k): self._resolve(config, prefix + str(k), v) for k, v in content.items()}

        elif isinstance(content, list):
            return [self._resolve(config, "%s.%d" % (prefix, i), v) for i, v in enumerate(content)]
        elif isinstance(content, DatasetReference):
            dataset = content.resolve(self.dataset)
            self.dependencies[path] = dataset
            return dataset
        
        return content

    @property
    def downloadHandler(self):
        return DownloadHandler.find(self.dataset, self.content["download"])

    def download(self, force=False):
        """Download all the resources (if available)"""
        logging.info("Downloading %s", self.content.get("name", self.dataset))

        # (1) Download direct resources
        if "download" in self.content:
            handler = self.downloadHandler
            destpath = handler.path(self.destpath)
            if destpath.exists() and not force:
                logging.info("File already downloaded [%s]", destpath)
            else:
                handler.download(destpath)

        # (2) Download dependencies
        success = True
        for dependency in self.dependencies.values():
            logging.debug("Downloading dependency %s", dependency)
            success &= dependency.download()

        return success

    def prepare(self):
        """Prepare the dataset
        
        Performs (basic) post-processing after the dataset has been downloaded
        """

        # Returned information
        p = {}
        
        # Will be used for updating the dataset registry
        r = {}

        # Update all dependencies
        for key, dependency in self.dependencies.items():
            dependency.prepare()

        p["id"] = self.dataset.id

        if "download" in self.content:
            handler = self.downloadHandler
            r["path"] = p["path"] = handler.path(self.destpath)
            self.dataset.files = handler.files(self.destpath)

        if self.version:
            p["version"] = r["version"] = self.version
            
        self.context.registry[self.dataset.id] = r
        self.context.registry.save()

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
    def repository(self):
        return self.dataset.repository

    @property
    def path(self) -> Path:
        path = Path(*self.dataset.id.split("."))
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
