from pathlib import Path
from csv import reader as csv_reader

from . import File, data, argument, documentation
from typing import Tuple, List


@argument("ignore", type=int, default=0)
@argument("names_row", type=int, default=-1)
@data()
class Generic(File):
    """A generic CSV file"""

    @documentation
    def columns(self):
        """Returns the list of field names (if any) or None"""
        if self.names_row < 0:
            return None

        with self.path.open("r") as fp:
            for i in range(self.ignore):
                fp.readline()

            for ix, row in enumerate(csv_reader(fp)):
                if ix == self.names_row:
                    return row


@argument("names_row", type=int, default=-1)
@argument("size_row", type=int, default=-1)
@argument("target", type=str, default=None)
@data()
class Matrix(Generic):
    """A numerical dataset"""

    @documentation
    def data(self) -> Tuple[List[str], "numpy.array"]:
        """Returns the list of fields and the numeric data


        Returns:
            [type]: List of fields
        """
        import numpy as np

        fields = []
        data = None if self.size_row >= 0 else []
        i = 0
        with self.path.open("r") as fp:
            for i in range(self.ignore):
                fp.readline()

            for ix, row in enumerate(csv_reader(fp)):
                if ix == self.size_row:
                    data = np.empty((int(row[0]), int(row[1])))
                elif ix == self.names_row:
                    fields = row
                else:
                    if self.size_row < 0:
                        data.append([float(x) for x in row])
                    else:
                        data[i] = [float(x) for x in row]
                        i += 1

        if self.size_row < 0:
            data = np.array(data)
        return fields, data
