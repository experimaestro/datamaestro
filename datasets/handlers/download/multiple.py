from datasets.handlers.download import DownloadHandler

class Simple(DownloadHandler):
    """Download multiple files or directories"""
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.list = self.definition["list"]

    def download(self, destination):
        for key, value in self.list.items():
            handler = DownloadHandler.find(self.repository, value)
            destpath = handler.resolve(destination)
            if destpath.exists() and not force:
                logging.info("File already downloaded [%s]", destpath)
            else:
                handler.download(destpath)
