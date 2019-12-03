"""Machine learning generic data formats"""
from typing import List
from pathlib import Path
from . import Generic, Data, Argument

@Data()
class Supervised(Generic): 
    """"""
    pass

@Argument("path", type=Path)
@Argument("classes")
@Data()
class FolderBased(Generic): pass