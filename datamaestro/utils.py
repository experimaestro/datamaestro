import logging
import os
import os.path as op
import json
from pathlib import PosixPath, Path
from shutil import rmtree
import shutil
import hashlib
from tqdm import tqdm

class TemporaryDirectory:
    def __init__(self, path: Path):
        self.delete = True
        self.path = path

    def __enter__(self):
        logging.debug("Creating directory %s", self.path)
        self.path.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, type, value, traceback):
        if self.delete:
            rmtree(self.path)

class FileChecker():
    def check(self, path: Path):
        """Check if the file is correct and throws an exception if not"""
        raise NotImplementedError()

class HashCheck(FileChecker):
    """Check a file against a hash"""
    def __init__(self, hashstr: str, hasher=hashlib.md5):
        self.hashstr = hashstr
        self.hasher = hasher

    def check(self, path: Path):
        """Check the given file

        returns true if OK
        """        
        with path.open("rb") as fp:
            hasher = self.hasher()
            chunk = fp.read(8192)
            while chunk:
                hasher.update(chunk)
                chunk = fp.read(8192)
        s = hasher.hexdigest()
        if s != self.hashstr:
            raise IOError(f"Digest do not match ({self.hashstr} vs {s})")


class CachedFile:
    """Represents a downloaded file that has been cached
    
    The file is automatically deleted when closed if keep is False
    and used is True
    """

    def __init__(self, path, keep=False, others=[]):
        self.path = path
        self.keep = keep
        self._force_delete = False
        self.others = []

    def __enter__(self):
        return self

    def force_delete(self):
        """Force the file to be deleted (even if an exception was thrown)"""
        self.force_delete = True

    def __exit__(self, exc_type, exc_value, traceback):
        # Avoid removing the file if an exception was thrown
        if not self.force_delete and exc_type is not None:
            logging.info("Keeping cache file %s (exception thrown)", self.path)
            return

        try:
            if not self.keep:
                logging.info("Deleting cache file %s", self.path)
                self.path.unlink()
                for other in self.others:
                    other.unlink()
        except Exception as e:
            logging.warning("Could not delete cached file %s [%s]", self.path, e)

class CurlDownloadReportHook(tqdm):
    """Report hook for tqdm when downloading from the Web with PyCURL"""

    def __init__(self, **kwargs):
        kwargs.setdefault("unit", "B")
        kwargs.setdefault("unit_scale", True)
        kwargs.setdefault("miniters", 1)
        super().__init__(**kwargs)

    def __call__(self, download_total, downloaded, upload_total, uploaded):
        """PyCURL callback"""
        if download_total is not None:
            self.total = download_total
        self.update(downloaded - self.n)  # will also set self.n = b * bsize

def downloadURL(url: str, path: Path, resume: bool=False):
    import pycurl
    try:
        with path.open(f"ab") as fp, CurlDownloadReportHook(desc="Downloading %s" % url) as reporthook:
            c = pycurl.Curl()
            c.setopt(pycurl.URL, url)
            if resume:
                c.setopt(pycurl.RESUME_FROM, os.path.getsize(path))
            c.setopt(pycurl.WRITEDATA, fp)
            c.setopt(pycurl.NOPROGRESS, 0)
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.XFERINFOFUNCTION, reporthook)
            c.perform()
            fp.close()
            shutil.move(path, dlpath)
    except pycurl.error as e:
        code = e.args[0]
        message = e.args[1]
        if code == pycurl.E_RANGE_ERROR:
            logging.error("Cannot resume download (%s) - starting all over", message)
            path.unlink()
            downloadURL(url, path, False)
        raise


def deprecated(message, f):
    from inspect import getframeinfo, stack

    def wrapped(*args, **kwargs):
        caller = getframeinfo(stack()[1][0])
        logging.warning(
            "called at %s:%d - %s" % (caller.filename, caller.lineno, message)
        )
        return f(*args, **kwargs)

    return wrapped


# --- JSON


class JsonContext:
    pass


class BaseJSONEncoder(json.JSONEncoder):
    def __init__(self):
        super().__init__()
        self.context = JsonContext()

    def default(self, o):
        return {
            key: value for key, value in o.__dict__.items() if not key.startswith("__")
        }


class JsonEncoder(BaseJSONEncoder):
    """Default JSON encoder"""

    def default(self, o):
        if isinstance(o, PosixPath):
            return str(o.resolve())
        return super().default(o)


class XPMEncoder(BaseJSONEncoder):
    """Experimaestro encoder"""

    def default(self, o):
        if isinstance(o, PosixPath):
            return {"$type": "path", "$value": str(o.resolve())}

        # Data object
        if hasattr(o.__class__, "__datamaestro__"):
            m = super().default(o)
            m["$type"] = o.__class__.__datamaestro__.id
            return m

        return super().default(o)
