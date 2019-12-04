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
