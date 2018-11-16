import logging
import os
import os.path as op

from json import JSONEncoder as BaseJSONEncoder
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

class JsonEncoder(BaseJSONEncoder):
    def default(self, o):
        if isinstance(o, PosixPath):
            return str(o.resolve())
        return o.__dict__    
