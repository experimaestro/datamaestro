"""Machine learning generic data formats"""
from typing import List
from pathlib import Path
from . import Base, argument


@argument("train", type=Base, help="The training dataset")
@argument("validation", type=Base, help="The validation dataset", required=False)
@argument("test", type=Base, help="The test dataset", required=False)
class Supervised(Base):
    pass


@argument("path", type=Path)
@argument("classes")
class FolderBased(Base):
    """Classification dataset where folders give the basis"""

    pass
