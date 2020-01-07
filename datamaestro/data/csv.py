from pathlib import Path
from csv import reader as csv_reader

from . import File, data, argument


@argument("ignore", type=int, default=0)
@argument("names_row", type=int, default=-1)
@data()
class Generic(File): 
    def columns(self):
        """Returns a couple (fields, matrix)"""
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
    def data(self):
        """Returns a couple (fields, matrix)"""
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


