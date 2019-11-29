from pathlib import Path
from datamaestro.definitions import Data, Argument

@Argument("id", type=str, help="The unique dataset ID")
@Data()
class Generic: pass

@Argument("path", type=Path)
@Data()
class File(Generic): 
    """A data file"""
    def open(self, mode):
        return self.path.open(mode)