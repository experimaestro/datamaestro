import logging
from datamaestro.handlers.dataset import DatasetHandler

# FIXME: Move to text
class Assessments(DatasetHandler):
    """TREC standard topics - one file in SGML format"""
    @property
    def destpath(self):
        return super().destpath.with_suffix(".dat")

    def prepare(self):
        return {
            "$type": "gov.nist.trec.adhoc.assessments",
            "path": self.destpath
        }

class Topics(DatasetHandler):
    """TREC standard topics - one file in SGML format"""
    @property
    def destpath(self):
        return super().destpath.with_suffix(".dat")
    def prepare(self):
        return {
            "$type": "gov.nist.trec.adhoc.topics",
            "path": self.destpath
        }


class Task(DatasetHandler):
    pass
