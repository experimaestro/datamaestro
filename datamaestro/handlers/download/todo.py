from pathlib import Path

from datamaestro.handlers.download import DownloadHandler

class Todo(DownloadHandler):
    def download(self, destination: Path):
        raise NotImplementedError("Download method not defined - please edit the definition file")