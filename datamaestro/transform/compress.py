from . import Transform

class Gunzip(Transform):
    def __call__(self, fileobj):
        import gzip
        return gzip.GzipFile(fileobj=fileobj)