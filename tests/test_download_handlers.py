
import unittest
import contextlib
import logging

import datamaestro.handlers.download.single as single

from datamaestro import Repository, Context, Dataset, DataFile


@contextlib.contextmanager
def make_temporary():
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)
context = None

class MyRepository(Repository):
    NAMESPACE = "test-simple"
    AUTHOR = """Benjamin Piwowarski <benjamin@piwowarski.fr>"""
    DESCRIPTION = """Repository with tests"""

class MainTest(unittest.TestCase):
    def test_single_(self):
        repository = MyRepository(context)
        datafile = DataFile(repository, "", "")
        dataset = Dataset(datafile)
        definition = {}
        single.File(dataset, definition)


if __name__ == '__main__':
    import sys
    with make_temporary() as dir:
        logging.info("Using %s as data directory", dir)
        context = Context(dir)
        unittest.main()
