"""Machine learning generic data formats"""
from typing import List
from pathlib import Path
from . import Generic, Data, Argument

@Argument("train", type=Generic, help="The training dataset")
@Argument("validation", type=Generic, help="The validation dataset", required=False)
@Argument("test", type=Generic, help="The test dataset", required=False)
@Data()
class Supervised(Generic): pass


@Argument("path", type=Path)
@Argument("classes")
@Data()
class FolderBased(Generic): pass