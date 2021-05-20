from pathlib import Path
from csv import reader as csv_reader
from . import File, data, argument, documentation
from datamaestro.definitions import Option
from typing import Tuple, List


@argument("ignore", type=int, default=0)
@argument("names_row", type=int, default=-1)
@data()
class Generic(File):
    """A generic CSV file"""

    ignore: Option[int] = 0
    names_row: Option[int] = 1

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


        Returns: List of fields
        """
        import numpy as np

        fields = []
        targets = None if self.size_row >= 0 or not self.target else []
        data = None if self.size_row >= 0 else []
        i = 0
        skipix = -1

        with self.path.open("r") as fp:
            for i in range(self.ignore):
                fp.readline()

            for ix, row in enumerate(csv_reader(fp)):
                if ix == self.size_row:
                    data = np.empty((int(row[0]), int(row[1])))
                elif ix == self.names_row:
                    fields = row
                    if self.target:
                        skipix = fields.index(self.target)
                else:
                    if self.size_row < 0:
                        data.append(
                            [float(x) for ix, x in enumerate(row) if ix != skipix]
                        )
                        if skipix >= 0:
                            targets.append(float(row[skipix]))
                    else:
                        data[i] = [float(x) for ix, x in enumerate(row) if ix != skipix]
                        if skipix >= 0:
                            targets[i] = float(row[skipix])
                        i += 1

        if self.size_row < 0:
            data = np.array(data)
            if targets:
                targets = np.array(targets)

        if self.target:
            return fields, data, targets

        return fields, data
