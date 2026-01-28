from pathlib import Path
from datamaestro import Repository, Context
import shutil
import logging
import pytest


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

    _repository = MyRepository(context)  # noqa: F841 - registered on creation

    yield context

    logging.info("Removing datamaestro test directory %s", dir)
    shutil.rmtree(dir)
