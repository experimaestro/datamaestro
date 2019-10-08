
import unittest
import contextlib
import logging
import tempfile
from pathlib import Path
import shutil

import datamaestro.handlers.download.single as single
from datamaestro import Repository, Context, Dataset, DataFile


TEST_PATH = Path(__file__).parent

@contextlib.contextmanager
def make_temporary():
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)

class TemporaryContext(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        "Hook method for setting up class fixture before running tests in the class."
        cls._dir = make_temporary()
        cls.dir = cls._dir.__enter__()
        cls.context = Context(Path(cls.dir))

    @classmethod
    def tearDownClass(cls):
        "Hook method for deconstructing the class fixture after running all tests in the class."
        cls._dir.__exit__(None, cls.dir, None)
        
class MyRepository(Repository):
    NAMESPACE = "test-simple"
    AUTHOR = """Benjamin Piwowarski <benjamin@piwowarski.fr>"""
    DESCRIPTION = """Repository with tests"""

class MainTest(TemporaryContext):

    def test_single_(self):
        repository = MyRepository(MainTest.context)
        
        f_definition = { "url": "file:///" + str(Path(__file__).resolve()), "__handler__": "/single:File"  }
        ds_definition = { "download": f_definition }

        datafile = DataFile.create(repository, "", None)
        dataset = Dataset(datafile, "single", ds_definition, None)
        dataset.download()


if __name__ == '__main__':
    import sys
    global context
    with make_temporary() as dir:
        logging.info("Using %s as data directory", dir)
        context = Context(dir)
        unittest.main()
