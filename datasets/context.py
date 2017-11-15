from pathlib import Path
import yaml
import sys
import importlib
import os
import hashlib

class Importer(importlib.abc.MetaPathFinder):
    def __init__(self, context):
        self.context = context

    def find_spec(self, fullname, path, target=None):
        if fullname.startswith("datasets.r."):
            names = fullname.split(".")[2:]
            names.insert(1, "module")
            path = Path(self.context.repositoriespath).joinpath(*names)
            pypath = path.with_suffix(".py")
            if pypath.is_file():
                ispackage = False
                path = pypath
            else:
                ispackage = True
                path = path.joinpath("__init__.py")
                if not path.is_file():
                    logging.warn("Could not find %s", path)
                    return None
            loader = importlib.machinery.SourceFileLoader(fullname, str(path))

            spec = importlib.machinery.ModuleSpec(fullname, loader, is_package=ispackage)
            return spec

        return None


class Compression:
    @staticmethod
    def extension(definition):
        if not definition: 
            return ""
        if definition == "gzip":
            return ".gz"

        raise Exception("Not handled compression definition: %s" % definition)

class RegistryEntry:
    def __init__(self, registry, key):    
        self.key = key
        self.dicts = []
        _key = ""   
        for subkey in self.key.split("."):
            _key = "%s.%s" % (_key, subkey) if _key else subkey
            if _key in registry.content:
                self.dicts.insert(0, registry.content[_key])
        
    def get(self, key, default):
        for d in self.dicts:
            if key in d:
                return d[key]
        return default
        
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

    def path(self):
        return self.path()

class Context:
    """
    Represents the configuration
    """
    MAINDIR = Path("~/datasets").expanduser()

    """Main settings"""
    def __init__(self, path: Path):
        self._path = path
        self.registry = Registry(self._path.joinpath("registry.yaml"))

        # FIXME: use enter/exit semantics
        sys.meta_path.append(Importer(self))

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

    @property
    def cachepath(self) -> Path:
        return self._path.joinpath("cache")

    def repositories(self):
        """Returns an iterator over definitions base directories"""
        from .data import Repository
        yielded = False
        for path in self.repositoriespath.iterdir():
            if path.is_dir():
                yield Repository(self, path)

        if not yielded: return []

    def datasets(self):
        """Returns an iterator over all files"""
        for repository in self.repositories():
            for dataset in repository:
                yield dataset

    def dataset(self, datasetid):
        from .data import Dataset
        return Dataset.find(self, datasetid)


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
                urllib.request.urlretrieve(url, tmppath)
                shutil.move(tmppath, dlpath)
            except:
                tmppath.unlink()
                raise


        return CachedFile(dlpath, urlpath)
        