import io
import logging

class Transform:
    def __init__(self, definition):
        self.definition = definition
       
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

class Gunzip(Transform):
    def __call__(self, fileobj):
        import gzip
        return gzip.GzipFile(fileobj=fileobj)

class TransformerList(Transform):
    def __init__(self):
        self.list = []

    def append(self, item):
        self.list.append(item)

    def __call__(self, fileobj):
        for item in self.list:
            fileobj = item(fileobj)
        return fileobj


class LineTransformStream(io.RawIOBase):
    """Transform line by line"""
    def __init__(self, fileobj, transform):
        self.current = ""
        self.offset = 1
        self.fileobj = fileobj
        self.transform = transform

    def readable(self):
        return True
    
    def readnext(self):
        # Read next line and transform
        self.offset = 0
        self.current = None
        while not self.current:
            line = self.fileobj.readline().decode("utf-8")
            if len(line) == 0: 
                return None

            self.current = self.transform(line).encode("utf-8")

    def readinto(self, b):
        """Read bytes into a pre-allocated, writable bytes-like object b and return the number of bytes read"""
        if self.current is None:
            return 0

        offset = 0
        lb = len(b)
        while lb > 0:
            while self.offset >= len(self.current):
                self.readnext()
                if self.current is None:
                    return offset

            # How many bytes to read from current line
            l = min(lb, len(self.current) - self.offset)

            b[offset:(offset+l)] = self.current[self.offset:(self.offset+l)]
            lb -= l
            offset += l
            self.offset += l

        return offset

class Replace(Transform):
    """Line by line transform"""
    def __init__(self, content):
        import re
        self.re = re.compile(content["pattern"])
        self.repl = content["repl"]
       
    def __call__(self, fileobj):
        return LineTransformStream(fileobj, lambda s: self.re.sub(self.repl, s))

class Filter(Transform):
    """Line by line transform"""
    def __init__(self, content):
        import re
        self.re = re.compile(content["pattern"])
       
    def filter(self, line):
        if self.re.search(line): 
            return line
        return ""

    def __call__(self, fileobj):
        return LineTransformStream(fileobj, self.filter)
