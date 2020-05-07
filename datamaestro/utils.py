import logging
import os
import os.path as op
import json
from pathlib import PosixPath, Path
from shutil import rmtree
import hashlib

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

class HashCheck:
    """Check a file against a hash"""
    def __init__(self, hashstr: str, hasher=hashlib.md5):
        self.hashstr = hashstr
        self.hasher = hasher

    def check(self, path: Path) -> bool:
        """Check the given file

        returns true if OK
        """        
        with path.open() as fp:
            hasher = self.hasher()
            hasher.update(fp)
        s = hasher.hexdigest()
        if s != self.hashstr:
            raise Exception(f"Digest do not match ({self.hashcheck} vs {s})")


class CachedFile:
    """Represents a downloaded file that has been cached"""

    def __init__(self, path, keep=False):
        self.path = path
        self.keep = keep

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if not self.keep:
                self.path.unlink()
        except Exception as e:
            logging.warning("Could not delete cached file %s [%s]", self.path, e)


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
