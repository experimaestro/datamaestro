import logging
import traceback
import importlib
import inspect

from datamaestro.context import Context, Repository

import unittest


class DatasetTests:
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

    def test_unique_id(self):
        """Test if IDs are unique within the module"""
        mapping = {}
        for dataset in self.repository:
            for id in dataset.aliases:
                mapping.setdefault(id, []).append(dataset.t)

        flag = True
        for key, values in mapping.items():
            if len(values) > 1:
                flag = False
                logging.error("Id %s has several mappings", key)
                for value in values:
                    filename = inspect.getfile(value)
                    lineno = inspect.getsourcelines(value)[1]
                    logging.error("%s:%d", filename, lineno)

        assert flag

    def test_datasets(self):
        """Check datasets integrity by preparing them (without downloading)

        Arguments:
            repository {Repository} -- The repository to check
        """
        for dataset in self.repository:
            with self.subTest(dataset_id=dataset.id):
                dataset.prepare(download=False)
