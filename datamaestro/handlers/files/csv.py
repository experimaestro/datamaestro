from pathlib import Path
from csv import reader as csv_reader
from . import File

class Matrix(File): 
    def __init__(self, path: Path, definition: dict):
        super().__init__(path, definition)
        self.target = definition.get("target", None)
        self.size_row = definition.get("size-row", -1)
        self.names_row = definition.get("names-row", -1)
        self.ignore = definition.get("ignore", 0)

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