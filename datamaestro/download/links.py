import logging
import os
from datamaestro.download import Download

class Links(Download):
    def __init__(self, varname, **links):
        super().__init__(varname)
        self.links = links

    @property
    def path(self):
        return self.definition.destpath

    def download(self, force=False):
        self.path.mkdir(exist_ok=True, parents=True)
        for key, value in self.links.items():
            value.download(force)
        
            path = value()
            dest = self.path / key

            if not dest.exists():
                os.symlink(path, dest)
