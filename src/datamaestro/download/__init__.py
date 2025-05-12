from typing import Union
from abc import ABC, abstractmethod
from datamaestro.definitions import AbstractDataset, DatasetAnnotation
from datamaestro.utils import deprecated
from attrs import define


def initialized(method):
    """Ensure the object is initialized"""

    def wrapper(self, *args, **kwargs):
        if not self._post:
            self._post = True
            self.postinit()
        return method(self, *args, **kwargs)

    return wrapper


@define(kw_only=True)
class SetupOptions:
    pass


class Resource(DatasetAnnotation, ABC):
    """
    Base class for all download handlers
    """

    def __init__(self, varname: str):
        self.varname = varname
        # Ensures that the object is initialized
        self._post = False
        self.definition: AbstractDataset = None

    def annotate(self, dataset: AbstractDataset):
        assert self.definition is None
        # Register has a resource download
        if self.varname in dataset.resources:
            raise AssertionError("Name %s already declared as a resource", self.varname)

        dataset.resources[self.varname] = self
        dataset.ordered_resources.append(self)
        self.definition = dataset

    def contextualize(self):
        """When using an annotation inline, uses the current dataset wrapper object"""
        from datamaestro.definitions import AbstractDataset

        wrapper = AbstractDataset.processing()
        self.annotate(wrapper)

    @property
    def context(self):
        return self.definition.context

    def postinit(self):
        pass

    def hasfiles(self):
        return True

    @abstractmethod
    def download(self, force=False):
        """Downloads the content"""
        ...

    @abstractmethod
    def prepare(self):
        """Prepares the dataset"""
        ...

    def setup(
        self,
        dataset: Union[AbstractDataset],
        options: SetupOptions = None,
    ):
        """Direct way to setup the resource (no annotation)"""
        self(dataset)
        return self.prepare()


# Keeps downwards compatibility
Download = Resource


class reference(Resource):
    def __init__(self, varname=None, reference=None):
        """References another dataset

        :param varname: The name of the variable
        :param reference: Another dataset
        """
        super().__init__(varname)
        assert reference is not None, "Reference cannot be null"
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
