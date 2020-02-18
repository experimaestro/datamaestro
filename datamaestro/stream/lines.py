import io
import re

from . import Transform


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

            b[offset : (offset + l)] = self.current[self.offset : (self.offset + l)]
            lb -= l
            offset += l
            self.offset += l

        return offset


class Replace(Transform):
    """Line by line transform"""

    def __init__(self, pattern, replacement):
        self.re = re.compile(pattern)
        self.repl = replacement

    def __call__(self, fileobj):
        return LineTransformStream(fileobj, lambda s: self.re.sub(self.repl, s))


class Filter(Transform):
    """Line by line transform"""

    def __init__(self, pattern):
        self.re = re.compile(pattern)

    def filter(self, line):
        if self.re.search(line):
            return line
        return ""

    def __call__(self, fileobj):
        return LineTransformStream(fileobj, self.filter)
