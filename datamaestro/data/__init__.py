from pathlib import Path
from datamaestro.definitions import Data, Argument

@Argument("id", type=str, help="The unique dataset ID")
@Data()
class Generic: 
    def initialize(self):
        """Method called before using the class"""
        pass

@Argument("path", type=Path)
@Data()
class File(Generic): 
    """A data file"""
    def open(self, mode):
        return self.path.open(mode)

@Argument("train", type=Generic, help="The training dataset")
@Argument("dev", type=Generic, help="The dev dataset", required=False)
@Argument("test", type=Generic, help="The test dataset", required=False)
@Data()
class Supervised(Generic): pass