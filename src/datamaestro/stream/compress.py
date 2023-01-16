from . import Transform


class Gunzip(Transform):
    def __call__(self, fileobj):
        import gzip

        return gzip.GzipFile(fileobj=fileobj)

    def path(self, path):
        if path.suffix == ".gz":
            return path.stem
        return path
