"""Programmatic registration of dataset families.

Some modules register many similar datasets in a loop (one per language,
per sub-collection, per split, …). Rather than writing a class per dataset,
or building classes by hand with ``type()``, they can use a
:class:`DatasetFactory`, which creates the :class:`~datamaestro.definitions.Dataset`
subclasses, applies the :class:`~datamaestro.definitions.dataset` decorator
and adds them to the module namespace so that datamaestro can discover
them::

    from datamaestro.data import File
    from datamaestro.download.single import FileDownloader
    from datamaestro.factory import DatasetFactory

    datasets = DatasetFactory(__name__, prefix="org.example")

    for lang in ["en", "fr"]:
        # Registers org.example.<lang>.vocabulary
        datasets.file(
            f"{lang}.vocabulary",
            File,
            VOCABULARY=FileDownloader("vocab.txt", f"https://.../{lang}.txt"),
        )

Unlike :mod:`dataset variants <datamaestro.variants>`, which share the
resources of a single dataset, each registered dataset has its own
resources (and download folder).

Plugins can subclass :class:`DatasetFactory` to add domain-specific
builders on top of :meth:`DatasetFactory.register`.
"""

import re
import sys
from typing import Callable, Optional

from datamaestro.data import Base
from datamaestro.definitions import Dataset, dataset
from datamaestro.download import Resource

DatasetClass = type[Dataset]


class DatasetFactory:
    """Creates and registers dataset classes in a module.

    :param module: Name of the module the datasets belong to (``__name__``);
        generated classes are added to its namespace so that datamaestro
        can discover them
    :param prefix: Prefix of the dataset ids (``<prefix>.<id>``); if None,
        ids are used as given (and must then be full ids)
    :param class_prefix: Prefix of the generated class names
    """

    def __init__(
        self, module: str, prefix: Optional[str] = None, class_prefix: str = ""
    ):
        self.module = module
        self.prefix = prefix
        self.class_prefix = class_prefix

    def full_id(self, ds_id: str) -> str:
        """Returns the full dataset id"""
        return f"{self.prefix}.{ds_id}" if self.prefix else ds_id

    def class_name(self, ds_id: str) -> str:
        """Derives a class name, not yet used in the module, from a dataset id

        Non-identifier characters are replaced by ``_``; if the name is
        already used (e.g. ``a-b`` and ``a_b``), a numeric suffix is added.
        Class names only matter for discovery: the dataset ids are always
        explicit.
        """
        namespace = vars(sys.modules[self.module])
        name = re.sub(r"\W", "_", ds_id)
        if self.class_prefix:
            name = f"{self.class_prefix}_{name}"
        candidate, count = name, 1
        while candidate in namespace:
            count += 1
            candidate = f"{name}_{count}"
        return candidate

    def register(
        self,
        ds_id: str,
        base: type[Base],
        config: Callable[[Dataset], Base],
        *,
        resources: Optional[dict[str, Resource]] = None,
        name: Optional[str] = None,
        doc: Optional[str] = None,
        **kwargs,
    ) -> DatasetClass:
        """Creates and registers a dataset class

        :param ds_id: The dataset id (relative to the prefix)
        :param base: The type of the dataset configuration
        :param config: The ``config(self)`` method of the dataset, returning
            an instance of ``base``
        :param resources: The resources of the dataset (attribute name ->
            resource); resources are accessed in ``config`` as
            ``self.<name>``
        :param name: The class name (derived from the id if None)
        :param doc: The dataset documentation
        :param kwargs: Other arguments for
            :class:`~datamaestro.definitions.dataset` (``url``, ``size``,
            ``doi``, …)
        :return: The registered dataset class
        """
        name = name or self.class_name(ds_id)
        namespace = dict(resources or {})
        namespace.update(
            config=config, __module__=self.module, __qualname__=name, __doc__=doc
        )
        cls = type(name, (Dataset,), namespace)
        cls = dataset(base, id=self.full_id(ds_id), **kwargs)(cls)
        setattr(sys.modules[self.module], name, cls)
        return cls

    def file(
        self,
        ds_id: str,
        base: type[Base],
        *,
        name: Optional[str] = None,
        doc: Optional[str] = None,
        url: Optional[str] = None,
        **resource: Resource,
    ) -> DatasetClass:
        """Registers a dataset whose configuration is built from one file

        The resource is given as a single keyword argument, whose name is
        the resource name (e.g. ``QUERIES=FileDownloader(...)``); the
        configuration is ``base.C(id=..., path=<resource path>)``, so
        ``base`` must have a ``path`` parameter.
        """
        if len(resource) != 1:
            raise ValueError(
                f"Expected exactly one resource, got {list(resource) or 'none'}"
            )
        ((varname, _),) = resource.items()

        def config(self) -> Base:
            return base.C(id=self.__dataset__.id, path=getattr(self, varname).path)

        return self.register(
            ds_id, base, config, resources=resource, name=name, doc=doc, url=url
        )
