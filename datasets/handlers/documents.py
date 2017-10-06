import logging
from datasets.data import Handler, DownloadHandler

class Collection(Handler):
    """Just a set of document collections"""
    def download(self):
        # Verify if the dataset has been downloaded
        pass
