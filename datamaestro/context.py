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
import progressbar

class Compression:
    @staticmethod
    def extension(definition):
        if not definition: 
            return ""
        if definition == "gzip":
            return ".gz"

        raise Exception("Not handled compression definition: %s" % definition)


class CachedFile():
    """Represents a downloaded file that has been cached"""
    def __init__(self, path, *paths):
        self.path = path
        self.paths = paths
    
    def discard(self):
        """Delete all cached files"""
        for p in chain([self.path], self.paths):
            try:
                p.unlink()
            except Exception as e:
                logging.warn("Could not delete cached file %s", p)


class DownloadReportHook:
    def __init__(self):
        self.pbar = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pbar:
            self.pbar.__exit__(exc_type, exc_val, exc_tb)

    def __call__(self, block_num, block_size, total_size):
        if not self.pbar:
            self.pbar = progressbar.bar.DataTransferBar(max_value=total_size if total_size > 0 else None).__enter__()

        downloaded = block_num * block_size
        if downloaded < total_size:
            self.pbar.update(downloaded)
        

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
                flatten_settings(self.settings, yaml.load(fp))
                


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

    def dataset(self, datasetid):
        """Get a dataset by ID"""
        from .data import Dataset
        return Dataset.find(self, datasetid)

    def preference(self, key, default=None):
        return self.settings.get(key, default)


    def download(self, url):
        """Downloads an URL"""
        hasher = hashlib.sha256(url.encode("utf-8"))

        self.cachepath.mkdir(exist_ok=True)
        path = self.cachepath.joinpath(hasher.hexdigest())
        urlpath = path.with_suffix(".url")
        dlpath = path.with_suffix(".dl")
    
        if urlpath.is_file():
            if urlpath.read_text() != url:
                # TODO: do something better
                raise Exception("Cached URL hash does not match. Clear cache to resolve")

        urlpath.write_text(url)
        if dlpath.is_file():
            logging.debug("Using cached file %s for %s", dlpath, url)
        else:

            logging.info("Downloading %s", url)
            tmppath = dlpath.with_suffix(".tmp")
            try:
                with DownloadReportHook() as reporthook:
                    urllib.request.urlretrieve(url, tmppath, reporthook)
                shutil.move(tmppath, dlpath)
            except:
                tmppath.unlink()
                raise


        return CachedFile(dlpath, urlpath)
        