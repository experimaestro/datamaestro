"""Machine learning generic data formats"""

from pathlib import Path
from typing import Generic, TypeVar, Optional
from experimaestro import field, Param, Meta
from . import Base

Train = TypeVar("Train", bound=Base)
Validation = TypeVar("Validation", bound=Base)
Test = TypeVar("Test", bound=Base)


class Supervised(Base, Generic[Train, Validation, Test]):
    train: Param[Base]
    """The training dataset"""

    validation: Param[Optional[Base]] = field(default=None, ignore_default=True)
    """The validation dataset (optional)"""

    test: Param[Optional[Base]] = field(default=None, ignore_default=True)
    """The training optional"""


class FolderBased(Base):
    """Classification dataset where folders give the basis"""

    classes: Param[list[str]]
    path: Meta[Path]
