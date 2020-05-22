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


class FileChecker:
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


def downloadURL(url: str, path: Path, resume: bool = False):
    import requests

    response = None
    pos = 0
    if path.is_file():
        pos = path.stat().st_size
        if resume and pos > 0:
            logging.warning("Trying to resume download from position %d", pos)
            response = requests.get(
                url, headers={"Range": f"bytes={pos}-"}, stream=True
            )
            if (
                response.status_code != requests.codes.PARTIAL_CONTENT
                or response.headers["Content-Range"] is None
            ):
                # Re-start with new request
                logging.warning("Could not resume download (range request failed)")
                path.unlink()
                response.close()
                response = None
                pos = 0
        else:
            path.unlink()

    if response is None:
        response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    if total_size > 0:
        total_size += pos

    CHUNK_SIZE = 1024
    with path.open("ab") as f, tqdm(
        initial=pos, total=total_size, unit_scale=True, unit="B"
    ) as t:
        for data in response.iter_content(chunk_size=CHUNK_SIZE):
            f.write(data)
            t.update(len(data))

        return


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
