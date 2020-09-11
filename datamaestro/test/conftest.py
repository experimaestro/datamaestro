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
