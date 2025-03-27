import regex
from typing import Iterator, Optional
from functools import cached_property
from attrs import field
import importlib
from omegaconf import OmegaConf
from functools import partial
from attrs import define
from datamaestro import BaseRepository
from datamaestro.definitions import AbstractDataset, DatasetWrapper
from datamaestro.data import Base


re_spec = regex.compile(r"""^(\w\.)+:(\w+)""")


@define
class RepositoryDataset:
    ids: list[str]
    """ID(s) of this dataset"""

    entry_point: str = field(validator=re_spec.match)
    """The entry point"""

    title: str
    """The full name of the dataset"""

    description: str
    """Description of the dataset"""

    url: Optional[str]
    """The URL"""

    groups: Optional[list[str]]
    """Groups to which this repository belongs"""


@define
class RepositoryAuthors:
    name: str
    email: str


@define
class RepositoryGroup:
    name: str
    tasks: list[str]
    tags: list[str]


@define
class RepositoryConfiguration:
    namespace: str
    authors: list[RepositoryAuthors]
    description: str
    groups: dict[str, RepositoryGroup]
    datasets: list[RepositoryDataset]


class YAMLDataset(AbstractDataset):
    def __init__(self, repository: "YAMLRepository", information: RepositoryDataset):
        super().__init__(repository)
        self.information = information
        self.id = self.information.ids[0]
        self.aliases = set(self.information.ids)

    @cached_property
    def wrapper(self) -> DatasetWrapper:
        module, func_name = self.information.entry_point.split(":")
        wrapper = getattr(importlib.import_module(module), func_name)
        return wrapper

    def _prepare(self) -> "Base":
        return self.wrapper()

    def download(self, **kwargs):
        return self.wrapper.download(**kwargs)


class YAMLRepository(BaseRepository):
    """YAML-based repository"""

    @property
    def id(self):
        return self.configuration.namespace

    @property
    def name(self):
        return self.configuration.namespace

    @cached_property
    def configuration(self):
        schema = OmegaConf.structured(RepositoryConfiguration)
        with importlib.resources.path(
            self.__class__.__module__, "datamaestro.yaml"
        ) as fp:
            conf = OmegaConf.load(fp)

        conf: RepositoryConfiguration = OmegaConf.merge(schema, conf)
        return conf

    def __iter__(self) -> Iterator["AbstractDataset"]:
        return map(partial(YAMLDataset, self), self.configuration.datasets)
