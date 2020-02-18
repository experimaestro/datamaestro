import io
import logging
from pathlib import Path


class Transform:
    @staticmethod
    def createFromPath(path: Path):
        if path.suffix == ".gz":
            from .compress import Gunzip

            return Gunzip()
        return Identity()

    def path(self, path):
        return path

    def __call__(self, input):
        raise NotImplementedError(
            "__call__ should be implemented in subclass %s" % type(self)
        )


class Identity(Transform):
    def __call__(self, fileobj):
        return fileobj


class TransformList(Transform):
    def __init__(self, *list):
        self.list = list

    def append(self, item):
        self.list.append(item)

    def path(self, path):
        for item in self.list:
            path = item.path(path)
        return path

    def __call__(self, fileobj):
        for item in self.list:
            fileobj = item(fileobj)
        return fileobj
