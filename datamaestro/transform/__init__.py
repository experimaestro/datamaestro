import io
import logging
from pathlib import Path

class Transform:
    def __init__(self, definition):
        self.definition = definition
       
    @staticmethod
    def createFromPath(path: Path):
        if path.suffix == ".gz":
            from .compress import Gunzip 
            return path.stem, Gunzip({})
        return path.name, Identity({})

    @staticmethod
    def create(repository, definition):
        t = TransformerList()
        for item in definition:
            if isinstance(item, list):
                name, d = item
                t.append(repository.findhandler("transform", name)(d))
            else:
                t.append(repository.findhandler("transform", item)({}))
        return t

    def __call__(self, input):
        raise NotImplementedError("__call__ should be implemented in subclass %s" % type(self))


class Identity(Transform):
    def __call__(self, fileobj):
        return fileobj


class TransformerList(Transform):
    def __init__(self):
        self.list = []

    def append(self, item):
        self.list.append(item)

    def __call__(self, fileobj):
        for item in self.list:
            fileobj = item(fileobj)
        return fileobj

