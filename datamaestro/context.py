from pathlib import Path
import yaml
import sys
import importlib
import os
import hashlib
import logging
import urllib
import shutil
from .registry import Registry
from itertools import chain
import pkg_resources
from tqdm import tqdm

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
    MAINDIR = Path("~/datamaestro").expanduser()

    """Main settings"""
    def __init__(self, path: Path = None):
        self._path = path or Context.MAINDIR
        self._dpath = Path(__file__).parents[1]
        self._repository = None
        self.registry = Registry(self.datapath / "registry.yaml")

        # Read preferences
        self.settings = {}
        settingsPath = self._path / "settings.yaml"
        if settingsPath.is_file():
            with settingsPath.open("r") as fp:
                flatten_settings(self.settings, yaml.load(fp, Loader=yaml.SafeLoader))
                
    @staticmethod
    def instance():
        return Context()

    @property
    def datapath(self):
        return self._path.joinpath("data")
        
    @property
    def cachepath(self) -> Path:
        return self._path.joinpath("cache")

    def repositories(self):
        """Returns the repository"""
        for entry_point in pkg_resources.iter_entry_points('datamaestro.repositories'):
            yield entry_point.load()(self)

    def repository(self, repositoryid):
        l = [x for x in pkg_resources.iter_entry_points('datamaestro.repositories', repositoryid)]
        if not l:
            raise Exception("No datasets repository named %s", repositoryid)
        if len(l) > 1:
            raise Exception("Too many datasets repository named %s", repositoryid)
        return l[0].load()(self)

    def datasets(self):
        """Returns an iterator over all files"""
        for repository in self.repositories():
            for dataset in repository:
                yield dataset

    def dataset(self, datasetid) -> ".data.Dataset":
        """Get a dataset by ID"""
        from .data import Dataset
        return Dataset.find(datasetid, context=self)

    def preference(self, key, default=None):
        return self.settings.get(key, default)

