import unittest
import contextlib
import logging
import tempfile
from pathlib import Path
import shutil

import datamaestro.download.single as single
from datamaestro import Repository, Context


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


class Definition:
    pass


class MainTest(TemporaryContext):
    def test_single_(self):
        repository = MyRepository(MainTest.context)

        url = "http://httpbin.org/html"
        downloader = single.filedownloader("test", url)
        downloader.definition = Definition()
        downloader.definition.datapath = Path(self.__class__.dir)
        downloader.context = self.__class__.context
        downloader.download()


if __name__ == "__main__":
    import sys

    global context
    with make_temporary() as dir:
        logging.info("Using %s as data directory", dir)
        context = Context(dir)
        unittest.main()
