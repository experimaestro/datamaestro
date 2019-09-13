import logging
import os
import os.path as op
import json
from pathlib import PosixPath, Path


def rm_rf(d):
    logging.debug("Removing directory %s" % d)
    for path in (op.join(d, f) for f in os.listdir(d)):
        if op.isdir(path):
            rm_rf(path)
        else:
            os.unlink(path)
    os.rmdir(d)

class TemporaryDirectory:
    def __init__(self, path: Path):
        self.delete = True
        self.path = path
    
    def __enter__(self):
        logging.debug("Creating directory %s", self.path)
        self.path.mkdir(parents=True, exist_ok=True)
        return self
    
    def __exit__(self ,type, value, traceback):
        if self.delete:
            rm_rf(self.path)


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

# --- JSON

class JsonContext:
    pass

class BaseJSONEncoder(json.JSONEncoder):
    def __init__(self):
        super().__init__()
        self.context = JsonContext()

    def default(self, o):
        from .data import Dataset
        if isinstance(o, Dataset):
            return o.__jsondict__(self.context)
        return {key: value for key, value in o.__dict__.items() if not key.startswith("__")}

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
            return {
                "$type": "path",
                "$value": str(o.resolve())
            }
        return super().default(o)

