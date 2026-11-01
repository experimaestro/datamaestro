"""Tests for :class:`datamaestro.factory.DatasetFactory`."""

import sys
import types
from pathlib import Path

import pytest
from experimaestro import Param

from datamaestro.data import Base, File
from datamaestro.download import reference
from datamaestro.download.single import FileDownloader
from datamaestro.factory import DatasetFactory


class Pair(Base):
    """A configuration referencing two files"""

    first: Param[File]
    second: Param[File]


@pytest.fixture
def module(context, monkeypatch):
    """A fresh module where datasets get registered"""
    name = "datamaestro_factory_test.config.family"
    mod = types.ModuleType(name)
    monkeypatch.setitem(sys.modules, name, mod)
    return mod


def _downloader(filename="data.txt"):
    return FileDownloader(filename, f"http://example.com/{filename}")


class TestRegister:
    def test_registers_in_module(self, module):
        datasets = DatasetFactory(module.__name__, prefix="org.example")

        cls = datasets.register(
            "a.b",
            File,
            lambda self: File.C(path=self.DATA.path),
            resources={"DATA": _downloader()},
            url="http://example.com",
            doc="Some data",
        )

        assert module.a_b is cls
        assert cls.__module__ == module.__name__
        assert cls.__qualname__ == cls.__name__ == "a_b"
        assert cls.__doc__ == "Some data"

        wrapper = cls.__dataset__
        assert wrapper.id == "org.example.a.b"
        assert wrapper.base is File
        assert wrapper.url == "http://example.com"
        assert list(wrapper.resources) == ["DATA"]

    def test_prepare(self, module):
        datasets = DatasetFactory(module.__name__, prefix="org.example")
        cls = datasets.register(
            "a",
            File,
            lambda self: File.C(path=self.DATA.path),
            resources={"DATA": _downloader("x.txt")},
        )

        config = cls.__dataset__.prepare()
        assert isinstance(config, File)
        assert config.id == "org.example.a"
        assert config.path == cls.__dataset__.datapath / "x.txt"

    def test_no_prefix(self, module):
        datasets = DatasetFactory(module.__name__)
        cls = datasets.register(
            "org.example.full", File, lambda self: File.C(path=Path("/x"))
        )
        assert cls.__dataset__.id == "org.example.full"

    def test_resources_are_per_dataset(self, module):
        """Each dataset has its own resources and data folder"""
        datasets = DatasetFactory(module.__name__, prefix="org.example")
        paths = []
        for lang in ["en", "fr"]:
            cls = datasets.register(
                lang,
                File,
                lambda self: File.C(path=self.DATA.path),
                resources={"DATA": _downloader()},
            )
            paths.append(cls.__dataset__.prepare().path)
        assert paths[0] != paths[1]

    def test_references(self, module):
        datasets = DatasetFactory(module.__name__, prefix="org.example")
        first = datasets.file("first", File, DATA=_downloader("1.txt"))
        second = datasets.file("second", File, DATA=_downloader("2.txt"))

        def config(self) -> Pair:
            return Pair.C(first=self.FIRST.config(), second=self.SECOND.config())

        pair = datasets.register(
            "pair",
            Pair,
            config,
            resources={"FIRST": reference(first), "SECOND": reference(second)},
        )

        config = pair.__dataset__.prepare()
        assert config.first.id == "org.example.first"
        assert config.first.path.name == "1.txt"
        assert config.second.id == "org.example.second"
        assert config.second.path.name == "2.txt"


class TestClassNames:
    def test_class_prefix(self, module):
        datasets = DatasetFactory(module.__name__, class_prefix="Fam")
        assert datasets.class_name("x.test-a") == "Fam_x_test_a"

    def test_explicit_name(self, module):
        datasets = DatasetFactory(module.__name__, prefix="org.example")
        cls = datasets.file("a", File, name="MyData", DATA=_downloader())
        assert module.MyData is cls

    def test_collisions(self, module):
        """Ids mapping to the same name get distinct classes"""
        datasets = DatasetFactory(module.__name__, prefix="org.example")
        a = datasets.file("a-b", File, DATA=_downloader())
        b = datasets.file("a_b", File, DATA=_downloader())

        assert a.__name__ == "a_b"
        assert b.__name__ == "a_b_2"
        assert module.a_b is a and module.a_b_2 is b
        assert a.__dataset__.id == "org.example.a-b"
        assert b.__dataset__.id == "org.example.a_b"


class TestFile:
    def test_file(self, module):
        datasets = DatasetFactory(module.__name__, prefix="org.example")
        cls = datasets.file("v", File, url="http://u", VOCAB=_downloader("v.txt"))

        wrapper = cls.__dataset__
        assert list(wrapper.resources) == ["VOCAB"]
        assert wrapper.url == "http://u"

        config = wrapper.prepare()
        assert isinstance(config, File)
        assert config.id == "org.example.v"
        assert config.path == wrapper.resources["VOCAB"].path

    @pytest.mark.parametrize(
        "resources", [{}, {"A": _downloader(), "B": _downloader()}]
    )
    def test_requires_one_resource(self, module, resources):
        datasets = DatasetFactory(module.__name__, prefix="org.example")
        with pytest.raises(ValueError, match="exactly one resource"):
            datasets.file("v", File, **resources)


def _documented_example() -> str:
    """Extracts the code of the "Dataset Families" example from the docs"""
    docs = Path(__file__).parents[3] / "docs" / "source" / "datasets.rst"
    text = docs.read_text().split("\nDataset Families\n", 1)[1]
    block = text.split(".. code-block:: python\n\n", 1)[1]
    lines = []
    for line in block.splitlines():
        if line and not line.startswith("    "):
            break
        lines.append(line[4:])
    return "\n".join(lines)


def test_documented_example(module, tmp_path):
    """The example of the documentation works as described"""
    if not (Path(__file__).parents[3] / "docs").is_dir():
        pytest.skip("Documentation not available")

    # experimaestro needs the source file of the classes
    source = tmp_path / "multilingual.py"
    source.write_text(_documented_example())
    module.__file__ = str(source)
    exec(compile(source.read_text(), str(source), "exec"), vars(module))

    registered = {
        cls.__dataset__.id: cls
        for cls in vars(module).values()
        if hasattr(cls, "__dataset__")
    }
    assert set(registered) == {
        "org.example.multilingual.en.corpus",
        "org.example.multilingual.en.train.topics",
        "org.example.multilingual.en.train",
        "org.example.multilingual.en.dev.topics",
        "org.example.multilingual.en.dev",
        "org.example.multilingual.sw.corpus",
        "org.example.multilingual.sw.dev.topics",
        "org.example.multilingual.sw.dev",
    }
    assert (
        module.Multilingual_en_train is registered["org.example.multilingual.en.train"]
    )

    task = registered["org.example.multilingual.sw.dev"].__dataset__.prepare()
    assert isinstance(task, module.Task)
    assert task.id == "org.example.multilingual.sw.dev"
    assert task.corpus.id == "org.example.multilingual.sw.corpus"
    assert task.corpus.path.name == "corpus.jsonl"
    assert task.topics.id == "org.example.multilingual.sw.dev.topics"
    assert task.topics.path.name == "topics.tsv"
