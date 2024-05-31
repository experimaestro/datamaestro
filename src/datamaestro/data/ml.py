"""Machine learning generic data formats"""
from typing import Generic, TypeVar, Optional
from pathlib import Path
from experimaestro import Param, Meta, argument
from . import Base

Train = TypeVar("Train", bound=Base)
Validation = TypeVar("Validation", bound=Base)
Test = TypeVar("Test", bound=Base)


class Supervised(Base, Generic[Train, Validation, Test]):
    train: Param[Base]
    """The training dataset"""

    validation: Param[Optional[Base]] = None
    """The validation dataset (optional)"""

    test: Param[Optional[Base]] = None
    """The training optional"""


@argument("classes")
class FolderBased(Base):
    """Classification dataset where folders give the basis"""

    path: Meta[Path]
