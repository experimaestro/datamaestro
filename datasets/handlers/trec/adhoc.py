import logging
from datasets.data import Handler, DownloadHandler

class Topics(Handler):
    """TREC standard topics - one file in SGML format"""
    @property
    def destpath(self):
        return super().destpath + ".dat"

class Task(Handler):
    pass
