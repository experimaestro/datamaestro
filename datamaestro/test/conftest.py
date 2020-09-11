from pathlib import Path
import contextlib
import unittest
import tempfile
from datamaestro import Repository, Context
import shutil
import logging
import pytest
import os
import shutil


class MyRepository(Repository):
    NAMESPACE = "test-simple"
    AUTHOR = """Benjamin Piwowarski <benjamin@piwowarski.fr>"""
    DESCRIPTION = """Repository with tests"""


repository = None

# @contextlib.contextmanager
# def make_temporary():
#     temp_dir = tempfile.mkdtemp()
#     try:
#         yield temp_dir
#     finally:
#         shutil.rmtree(temp_dir)


# class TemporaryContext(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls):
#         "Hook method for setting up class fixture before running tests in the class."
#         cls._dir = make_temporary()
#         cls.dir = cls._dir.__enter__()

#     @classmethod
#     def tearDownClass(cls):
#         "Hook method for deconstructing the class fixture after running all tests in the class."
#         cls._dir.__exit__(None, cls.dir, None)


@pytest.fixture(scope="session")
def context(tmp_path_factory):
    """Sets a temporary main directory"""
    logging.warning("In context fixture")
    dir = tmp_path_factory.mktemp("datamaestro-data")
    context = Context(Path(dir))
    logging.info("Created datamaestro test directory %s", dir)

    repository = MyRepository(context)

    yield context

    logging.info("Removing datamaestro test directory %s", dir)
    shutil.rmtree(dir)
