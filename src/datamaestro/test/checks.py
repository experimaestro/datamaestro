import logging
import importlib
import inspect
from datamaestro.context import Context
from experimaestro.tools.documentation import DocumentationAnalyzer


class DatamaestroAnalyzer(DocumentationAnalyzer):
    """Documentation analyzer that excludes @dataset-annotated classes.

    Dataset definitions (classes decorated with ``@dataset``) are documented
    via ``dm:datasets`` Sphinx directives, not ``autoxpmconfig``. This
    subclass filters them from the undocumented list after analysis.
    """

    def analyze(self):
        super().analyze()
        filtered = []
        for fqn in self.undocumented:
            module_name, class_name = fqn.rsplit(".", 1)
            try:
                mod = importlib.import_module(module_name)
                cls = getattr(mod, class_name)
                if not hasattr(cls, "__dataset__"):
                    filtered.append(fqn)
            except (ImportError, AttributeError):
                filtered.append(fqn)
        self.undocumented = filtered


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
