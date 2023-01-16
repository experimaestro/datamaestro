from pathlib import Path

from datamaestro.download import Download


class Todo(Download):
    def download(self, destination: Path):
        raise NotImplementedError(
            "Download method not defined - please edit the definition file"
        )
