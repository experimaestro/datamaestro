from datamaestro.definitions import AbstractDataset, DatasetAnnotation, DatasetWrapper
from datamaestro.utils import deprecated


def initialized(method):
    """Ensure the object is initialized"""

    def wrapper(self, *args, **kwargs):
        if not self._post:
            self._post = True
            self.postinit()
        return method(self, *args, **kwargs)

    return wrapper


class Download(DatasetAnnotation):
    """
    Base class for all download handlers
    """

    def __init__(self, varname: str):
        self.varname = varname
        # Ensures that the object is initialized
        self._post = False

    def annotate(self, dataset: AbstractDataset):
        # Register has a resource download
        if self.varname in dataset.resources:
            raise AssertionError("Name %s already declared as a resource", self.varname)

        dataset.resources[self.varname] = self
        self.definition = dataset

    @property
    def context(self):
        return self.definition.context

    def postinit(self):
        pass

    def hasfiles(self):
        return True

    def download(self, force=False):
        """Downloads the content"""
        raise NotImplementedError()


class reference(Download):
    def __init__(self, varname, reference):
        super().__init__(varname)
        self.reference = reference

    def prepare(self):
        v = self.reference.prepare()
        if isinstance(v, AbstractDataset):
            return v().prepare()
        return v

    def download(self, force=False):
        self.reference.__datamaestro__.download(force)

    def hasfiles(self):
        # We don't really have files
        return False


Reference = deprecated("Use @reference instead of @Reference", reference)
