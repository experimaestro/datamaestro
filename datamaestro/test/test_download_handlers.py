import unittest
import logging
from pathlib import Path
import shutil
import datamaestro.download.single as single
from datamaestro import Repository, Context
from .conftest import MyRepository


TEST_PATH = Path(__file__).parent


class Definition:
    pass


def test_filedownloader(context):
    repository = MyRepository(context)

    url = "http://httpbin.org/html"
    downloader = single.filedownloader("test", url)
    downloader.definition = Definition()
    downloader.definition.datapath = Path(context._path)
    downloader.context = context
    downloader.download()
