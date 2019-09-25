from pathlib import Path

from datamaestro.handlers.transform import Transform

class File: 
    def __init__(self, path: Path, definition: dict):
        self.path = path
        if "transforms" in definition:
            self.transformer = Transform.create(self.repository, definition["transforms"])
        else:
            self.transformer = Transform.createFromPath(self.path)



    def open(self, mode):
        return self.transformer(self.path.open(mode))