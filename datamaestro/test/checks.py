import logging
import traceback
import importlib

from datamaestro.definitions import Repository
from datamaestro.context import Context

import unittest

class DatasetTests():
    @classmethod
    def setUpClass(cls):
        context = Context.instance()
        module = importlib.import_module(cls.__module__.split(".")[0])
        logging.info("Setting up %s", module.Repository(context))
        cls.__DATAMAESTRO_REPOSITORY__ = module.Repository(context)

    @property
    def repository(self):
        return self.__class__.__DATAMAESTRO_REPOSITORY__

    def test_datafiles(self):
        for context, file_id, package in self.repository._modules():
            with self.subTest(package=package):
                importlib.import_module(package)

    def test_datasets(self):
        """Check datasets integrity by preparing them (without downloading)
        
        Arguments:
            repository {Repository} -- The repository to check
        """
        for dataset in self.repository:
            with self.subTest(dataset_id=dataset.id):
                dataset.prepare(download=False)
