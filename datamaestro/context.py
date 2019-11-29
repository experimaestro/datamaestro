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
import json
import pkg_resources
from tqdm import tqdm
from .utils import CachedFile

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
            raise Exception("Too many datasets repository named %s", repositoryid)
        return l[0].load()(self)

    def datasets(self):
        """Returns an iterator over all files"""
        for repository in self.repositories():
            for dataset in repository:
                yield dataset

    def dataset(self, datasetid) -> ".data.DatasetDefinition":
        """Get a dataset by ID"""
        from .definitions import Repository
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
        