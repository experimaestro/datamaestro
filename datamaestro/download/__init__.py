from datamaestro.definitions import DataAnnotation, DatasetWrapper
from datamaestro.utils import deprecated


def initialized(method):
    """Ensure the object is initialized"""

    def wrapper(self, *args, **kwargs):
        if not self._post:
            self._post = True
            self.postinit()
        return method(self, *args, **kwargs)

    return wrapper


class Download(DataAnnotation):
    """
    Base class for all download handlers
    """

    def __init__(self, varname: str):
        self.varname = varname
        # Ensures that the object is initialized
        self._post = False

    def annotate(self):
        # Register has a resource download
        if self.varname in self.definition.resources:
            raise AssertionError("Name %s already declared as a resource", self.varname)

        self.definition.resources[self.varname] = self

    def postinit(self):
        pass

    def hasfiles(self):
        return True

    def download(self, force=False):
        """Downloads the content"""
        raise NotImplementedError()


class DatasetWrapper:
    def __init__(self, annotation, t):
        self.t = t
        d = DatasetDefinition(t)
        self.__datamaestro__ = d
        d.base = annotation.base
        d.update(annotation.base.__datamaestro__)

        # Removes module_name.config prefix
        path = t.__module__.split(".", 2)[2]
        d.id = "%s.%s" % (path, annotation.id or t.__name__.lower())
        d.aliases.add(d.id)

    def __call__(self, *args, **kwargs):
        self.t(*args, **kwargs)

    def __getattr__(self, key):
        return FutureAttr(self.__datamaestro__, [key])


class reference(Download):
    def __init__(self, varname, reference):
        super().__init__(varname)
        self.reference = reference

    def prepare(self):
        v = self.reference.__datamaestro__.prepare()
        if isinstance(v, DatasetWrapper):
            return v().prepare()
        return v

    def download(self, force=False):
        self.reference.__datamaestro__.download(force)

    def hasfiles(self):
        # We don't really have files
        return False


Reference = deprecated("Use @reference instead of @Reference", reference)
