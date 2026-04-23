import logging
import importlib
import inspect
import pkgutil
import re
from typing import Iterator, Optional, Set, Tuple

from datamaestro.context import Context
from experimaestro.tools.documentation import DocumentationAnalyzer


_DATASETS_DIRECTIVE = re.compile(
    r"^\s*\.\.\s+dm:datasets::\s+(\S+)",
    re.MULTILINE,
)
_REPOSITORY_DIRECTIVE = re.compile(
    r"^\s*\.\.\s+dm:repository::\s+(\S+)",
    re.MULTILINE,
)


def _config_module_id(module_name: str) -> Optional[str]:
    """Convert ``<pkg>.config.<path>`` to ``<path>``.

    Mirrors the argument expected by ``.. dm:datasets::`` (which addresses
    a module inside a repository's ``config`` package). Returns ``None``
    for modules outside a ``config`` subpackage.
    """
    marker = ".config."
    idx = module_name.find(marker)
    if idx >= 0:
        return module_name[idx + len(marker) :]
    if module_name.endswith(".config"):
        return ""
    return None


class DatamaestroAnalyzer(DocumentationAnalyzer):
    """Documentation analyzer that also verifies ``@dataset`` coverage.

    Dataset classes (decorated with ``@dataset``) typically don't subclass
    experimaestro's ``Config``, so the base :class:`DocumentationAnalyzer`
    never sees them. This subclass discovers every ``@dataset`` class
    under the target packages' ``config`` tree and flags any whose config
    module is not referenced by a ``dm:datasets`` (or ``dm:repository``)
    directive in the RST sources.
    """

    def _covered_modules(self) -> Set[str]:
        """Return the set of config-module ids referenced by ``dm:datasets``
        across all RST files under ``self.doc_path.parent``. Adds the
        sentinel ``""`` when any ``dm:repository`` directive is present
        (which documents the whole repository)."""
        docs_root = self.doc_path.parent
        modules: Set[str] = set()
        for rst in docs_root.rglob("*.rst"):
            try:
                text = rst.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            modules.update(_DATASETS_DIRECTIVE.findall(text))
            if _REPOSITORY_DIRECTIVE.search(text):
                modules.add("")
        return modules

    def _iter_dataset_classes(self) -> Iterator[Tuple[str, type]]:
        """Yield ``(fqn, cls)`` for every ``@dataset`` class declared in
        one of ``self.modules``' ``config`` subpackages, respecting the
        ``excluded`` skip list."""
        for pkg_name in self.modules:
            try:
                importlib.import_module(pkg_name)
            except Exception:
                logging.exception("Could not import package %s", pkg_name)
                continue

            config_pkg_name = f"{pkg_name}.config"
            try:
                config_pkg = importlib.import_module(config_pkg_name)
            except Exception:
                continue

            # The top-level config package itself (datasets defined in
            # ``<pkg>/config/__init__.py`` aren't reached by walk_packages).
            yield from self._dataset_classes_in(config_pkg)

            if not hasattr(config_pkg, "__path__"):
                continue
            for info in pkgutil.walk_packages(
                config_pkg.__path__, prefix=f"{config_pkg_name}."
            ):
                try:
                    mod = importlib.import_module(info.name)
                except Exception:
                    logging.exception("Could not import module %s", info.name)
                    self.parsing_errors.append(
                        f"Module {info.name} could not be loaded"
                    )
                    continue
                yield from self._dataset_classes_in(mod)

    def _dataset_classes_in(self, mod) -> Iterator[Tuple[str, type]]:
        seen: Set[str] = set()
        for name, obj in vars(mod).items():
            if not isinstance(obj, type):
                continue
            if not hasattr(obj, "__dataset__"):
                continue
            # Only count the class where it was defined, so a re-export in
            # an ``__init__`` doesn't double-report.
            if obj.__module__ != mod.__name__:
                continue
            fqn = f"{obj.__module__}.{obj.__qualname__}"
            if fqn in seen:
                continue
            if any(fqn.startswith(f"{x}.") or fqn == x for x in self.excluded):
                continue
            seen.add(fqn)
            yield fqn, obj

    def analyze(self):
        super().analyze()
        covered = self._covered_modules()
        wildcard = "" in covered

        # Remove any ``@dataset`` classes the parent may have picked up —
        # they're documented via ``dm:datasets``, not ``autoxpmconfig``.
        filtered = []
        for fqn in self.undocumented:
            module_name, class_name = fqn.rsplit(".", 1)
            try:
                mod = importlib.import_module(module_name)
                cls = getattr(mod, class_name)
            except (ImportError, AttributeError):
                filtered.append(fqn)
                continue
            if hasattr(cls, "__dataset__"):
                continue
            filtered.append(fqn)
        self.undocumented = filtered

        # Add ``@dataset`` classes whose config module isn't referenced by
        # a ``dm:datasets`` (or ``dm:repository``) directive.
        if not wildcard:
            for fqn, cls in self._iter_dataset_classes():
                module_id = _config_module_id(cls.__module__)
                if module_id is not None and module_id in covered:
                    continue
                self.undocumented.append(fqn)


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
        """Check datasets integrity by preparing them (without downloading).

        For variant families, prepares the first enumerable variant —
        validating every combination would be prohibitive and not
        representative of the family's correctness.

        Arguments:
            repository {Repository} -- The repository to check
        """
        for dataset in self.repository:
            with self.subTest(dataset_id=dataset.id):
                variants = getattr(dataset, "variants", None)
                if variants is not None:
                    variant_kwargs = next(iter(variants.enumerate()), None)
                    config = dataset.prepare(
                        download=False, variant_kwargs=variant_kwargs
                    )
                else:
                    config = dataset.prepare(download=False)
                config.__xpm__.validate()
