"""Tests for the dataset catalog"""

import shutil

from typing import Optional

import pytest
from experimaestro import Meta, Param, field

from datamaestro.catalog import Catalog, DatasetEntry
from datamaestro.context import Context, Repository
from datamaestro.data import Base
from datamaestro.definitions import Dataset, dataset as dataset_dec
from datamaestro.variants import AxesVariants, Axis, Variants


class Corpus(Base):
    """A made-up configuration type"""

    lang: Param[str]
    streaming: Meta[bool] = field(default=False, ignore_default=True)
    limit: Param[Optional[int]] = field(default=None, ignore_default=True)


class CorpusVariants(AxesVariants):
    lang = Axis(["en", "fr"])
    """Language of the documents"""

    streaming = Axis([False, True], default=True, type=bool, in_id=False)
    limit = Axis(type=int, default=None, elide_default=True)


class Presets(Variants):
    def resolve(self, **kwargs):
        return {"size": kwargs.get("size", "small")}

    def parse_selector(self, selector):
        return {}

    def format_selector(self, kwargs):
        return ""

    def enumerate(self):
        yield {"size": "small"}
        yield {"size": "huge"}


class FixedRepository(Repository):
    NAMESPACE = "test-catalog"
    AUTHOR = "test"
    DESCRIPTION = "Fixed-list repository for catalog tests"

    def __init__(self, context, datasets=()):
        super().__init__(context)
        self.datasets = list(datasets)

    def __iter__(self):
        for dataset in self.datasets:
            if isinstance(dataset, Exception):
                raise dataset
            yield dataset


def _wrapper(cls, name, **kwargs):
    cls.__module__ = "datamaestro.config.test_catalog"
    cls.__qualname__ = cls.__name__ = name
    return dataset_dec(url="http://example.com", **kwargs)(cls).__dataset__


def _flat():
    class Flat(Dataset):
        """A flat corpus

        Documents about made-up topics.
        """

        def config(self) -> Corpus:
            return Corpus.C(lang="en")

    wrapper = _wrapper(Flat, "flat")
    wrapper.tags |= {"text", "small"}
    wrapper.tasks.add("retrieval")
    return wrapper


def _family():
    class Family(Dataset):
        """A multilingual corpus"""

        def config(self, **kwargs) -> Corpus:
            return Corpus.C(**kwargs)

    wrapper = _wrapper(Family, "family", variants=CorpusVariants)
    wrapper.tags.add("text")
    return wrapper


def _presets():
    class WithPresets(Dataset):
        """Some presets"""

        def config(self, **kwargs) -> Corpus:
            return Corpus.C(lang="en")

    return _wrapper(WithPresets, "presets", variants=Presets())


@pytest.fixture(autouse=True)
def environment(tmp_path, monkeypatch):
    """Caches the catalog in a temporary folder (not in the environment)"""
    from datamaestro import catalog

    folder = tmp_path / "environment"
    monkeypatch.setattr(catalog, "_environment_cache_dir", lambda: folder)
    return folder


@pytest.fixture
def repository(context, monkeypatch):
    repository = FixedRepository(context)
    for wrapper in (_flat(), _family(), _presets()):
        wrapper.repository = repository
        repository.datasets.append(wrapper)
    monkeypatch.setattr(Context, "repositories", lambda self: iter([repository]))
    return repository


@pytest.fixture
def catalog(context, repository):
    return Catalog.build(context)


def test_entry(catalog):
    assert len(catalog) == 3
    entry = catalog.get("test_catalog.flat")
    assert entry.name == "A flat corpus"
    assert entry.description.strip() == "Documents about made-up topics."
    assert entry.repository == "test-catalog"
    assert entry.module == "datamaestro.config.test_catalog"
    assert entry.tags == ["small", "text"]
    assert entry.tasks == ["retrieval"]
    assert entry.config_type == f"{__name__}.Corpus"
    assert entry.types[0] == str(Corpus.__getxpmtype__().identifier)
    assert entry.url == "http://example.com"
    assert entry.path == "test-catalog/test_catalog/flat"
    assert entry.variants is None


def test_axes_variants(catalog):
    variants = catalog.get("test_catalog.family[lang=fr]").variants
    lang, streaming, limit = variants.axes

    assert (lang.name, lang.domain, lang.required) == ("lang", ["en", "fr"], True)
    assert lang.description == "Language of the documents"
    assert lang.type == "str"

    assert streaming.domain == ["false", "true"]
    assert (streaming.default, streaming.in_id) == ("true", False)

    assert limit.domain is None
    assert (limit.default, limit.required, limit.elide_default) == (
        "null",
        False,
        True,
    )
    assert "Variants" in variants.documentation


def test_other_variants(catalog):
    variants = catalog.get("test_catalog.presets").variants
    assert variants.axes is None
    assert variants.examples == [{"size": "small"}, {"size": "huge"}]


@pytest.mark.parametrize(
    "query,expected",
    [
        ("", {"flat", "family", "presets"}),
        ("tag:text", {"flat", "family"}),
        ("tag:text task:retrieval", {"flat"}),
        ("repo:test-catalog", {"flat", "family", "presets"}),
        ("MULTILINGUAL", {"family"}),
        ("made-up", {"flat"}),  # description
        ("fr", {"family"}),  # variant value
        ("huge", {"presets"}),
        ("id:family", {"family"}),
        (f"type:{Corpus.__getxpmtype__().identifier}", {"flat", "family", "presets"}),
        ("tag:(", set()),  # invalid regex
        ("nothing-matches", set()),
    ],
)
def test_search(catalog, query, expected):
    assert {entry.id.rsplit(".", 1)[1] for entry in catalog.search(query)} == expected


def test_facets(catalog):
    facets = catalog.facets()
    assert facets["tag"] == {"text": 2, "small": 1}
    assert facets["task"] == {"retrieval": 1}
    assert facets["repository"] == {"test-catalog": 3}

    assert catalog.facets(catalog.search("tag:small"))["tag"] == {
        "text": 1,
        "small": 1,
    }


def test_load_dataset(catalog, repository):
    entry = catalog.get("test_catalog.family")
    assert entry.dataset() is repository.datasets[1]


def test_errors(context, repository):
    repository.datasets.append(RuntimeError("broken module"))
    catalog = Catalog.build(context)
    assert len(catalog) == 3
    (error,) = catalog.errors
    assert error.source == "test-catalog"
    assert error.error == "RuntimeError: broken module"


def test_json_round_trip(catalog, tmp_path):
    path = tmp_path / "catalog.json"
    catalog.save(path)

    import json

    loaded = Catalog.from_json(json.loads(path.read_text()))
    assert [e.to_json() for e in loaded] == [e.to_json() for e in catalog]
    assert isinstance(loaded.get("test_catalog.family"), DatasetEntry)


def test_cache(context, repository, monkeypatch, environment):
    assert Catalog.cache_path(context) == environment / "catalog.json"
    builds = []
    build = Catalog.build
    monkeypatch.setattr(
        Catalog, "build", staticmethod(lambda c: builds.append(1) or build(c))
    )

    assert len(Catalog.load(context)) == 3
    assert len(Catalog.load(context)) == 3
    assert len(builds) == 1

    Catalog.load(context, refresh=True)
    assert len(builds) == 2

    # A change in the repository rebuilds the catalog
    monkeypatch.setattr(FixedRepository, "version", classmethod(lambda cls: "2.0"))
    Catalog.load(context)
    assert len(builds) == 3


# --- Web interface helpers


def test_make_query(catalog):
    from datamaestro.gui import make_query

    query = make_query("corpus", {("tag", "text"), ("repository", "test-catalog")})
    assert query == ["corpus", r"repository:^test\-catalog$", "tag:^text$"]
    assert {e.id for e in catalog.search(query)} == {
        "test_catalog.flat",
        "test_catalog.family",
    }
    # Facet values match exactly
    assert catalog.search(make_query("", {("tag", "tex")})) == []


@pytest.mark.parametrize(
    "values,expected",
    [
        ({"lang": "fr"}, "test_catalog.family[lang=fr]"),
        # in_id=False axis is not in the id, elided default
        ({"lang": "en", "streaming": "false", "limit": None}, "[lang=en]"),
        ({"lang": "en", "limit": "10"}, "[lang=en,limit=10]"),
    ],
)
def test_variant_id(catalog, values, expected):
    from datamaestro.gui import variant_id

    dataset_id, error = variant_id(catalog.get("test_catalog.family"), values)
    assert error is None
    assert dataset_id.endswith(expected)


def test_variant_id_errors(catalog):
    from datamaestro.gui import variant_id

    entry = catalog.get("test_catalog.family")
    assert variant_id(entry, {}) == (None, "missing required variant axis: 'lang'")
    assert variant_id(entry, {"lang": "de"})[0] is None


def test_rows(catalog):
    from ds_cache_cleaner import EntryMetadata

    from datamaestro.gui import human_size, rows

    downloads = {"test-catalog/test_catalog/flat": EntryMetadata(path="x", size=2048)}
    by_id = {row["id"]: row for row in rows(catalog, downloads)}
    assert by_id["test_catalog.flat"]["size_label"] == "2.0 KB"
    assert by_id["test_catalog.flat"]["variants"] == ""
    assert by_id["test_catalog.family"]["variants"] == "lang,streaming,limit"
    assert by_id["test_catalog.family"]["size"] is None
    assert by_id["test_catalog.presets"]["variants"] == "…"

    assert human_size(10) == "10 B"
    assert human_size(3 * 1024**3) == "3.0 GB"


def test_cli_search(context, repository, monkeypatch):
    from click.testing import CliRunner

    from datamaestro.__main__ import cli

    # The command line creates its own context
    monkeypatch.setattr(Context, "INSTANCE", None)
    repository.datasets.append(RuntimeError("broken module"))
    runner = CliRunner()
    data = ["--data", str(context._path)]

    result = runner.invoke(cli, [*data, "search", "--refresh", "tag:text"])
    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "[test-catalog] test_catalog.flat (Corpus)",
        "[test-catalog] test_catalog.family[lang,limit,streaming] (Corpus)",
    ]

    Context.INSTANCE = None
    result = runner.invoke(cli, [*data, "search", "--errors", "huge"])
    assert result.stdout.splitlines() == [
        "[test-catalog] test_catalog.presets[...] (Corpus)"
    ]
    assert "test-catalog: RuntimeError: broken module" in result.stderr


def test_cache_path_not_writable(context, repository, monkeypatch):
    """Falls back to the datamaestro folder if the environment is read-only"""
    from datamaestro import catalog

    monkeypatch.setattr(catalog.os, "access", lambda path, mode: False)
    path = Catalog.cache_path(context)
    assert path.parent == context.cachepath / "catalog"
    assert path == Catalog.cache_path(context)

    # Another repository version: another file
    monkeypatch.setattr(FixedRepository, "version", classmethod(lambda cls: "2.0"))
    assert Catalog.cache_path(context) != path


def test_temporary_environment(context, repository, monkeypatch, tmp_path):
    """uv temporary environments use the datamaestro folder"""
    from datamaestro import catalog

    folder = tmp_path / "uv" / "builds-v0" / ".tmpABC" / "share" / "datamaestro"
    monkeypatch.setattr(catalog, "_environment_cache_dir", lambda: folder)
    assert Catalog.cache_path(context).parent == context.cachepath / "catalog"
    assert not folder.exists()


def test_shared_caches_are_pruned(context, repository, monkeypatch):
    from datamaestro import catalog

    monkeypatch.setattr(catalog.os, "access", lambda path, mode: False)
    monkeypatch.setattr(catalog, "MAX_SHARED_CACHES", 2)
    folder = context.cachepath / "catalog"
    shutil.rmtree(folder, ignore_errors=True)

    for version in ["1", "2", "3"]:
        monkeypatch.setattr(
            FixedRepository, "version", classmethod(lambda cls, v=version: v)
        )
        Catalog.load(context)
        last = Catalog.cache_path(context)

    files = list(folder.glob("*.json"))
    assert len(files) == 2 and last in files


def test_with_downloads(catalog):
    """Downloaded datasets not defined in the environment are listed"""
    from ds_cache_cleaner import EntryMetadata

    downloads = {
        # Defined in the environment
        "test-catalog/test_catalog/flat": EntryMetadata(
            path="test-catalog/test_catalog/flat", size=1
        ),
        # Defined by another version of the repository
        "test-catalog/test_catalog/gone": EntryMetadata(
            path="test-catalog/test_catalog/gone",
            description="test_catalog.gone: A removed corpus",
            metadata={
                "id": "test_catalog.gone",
                "repository": "test-catalog",
                "repository_version": "0.9",
                "module": "datamaestro.config.test_catalog",
                "tags": ["text"],
            },
        ),
    }

    full = catalog.with_downloads(downloads)
    assert len(catalog) == 3 and len(full) == 4

    gone = full.get("test_catalog.gone")
    assert not gone.available
    assert gone.name == "A removed corpus"
    assert gone.repository_version == "0.9"
    assert gone.path == "test-catalog/test_catalog/gone"
    assert full.get("test_catalog.flat").available

    assert {e.id for e in full.search("tag:text removed")} == {"test_catalog.gone"}


def test_status(catalog):
    from ds_cache_cleaner import EntryMetadata

    from datamaestro import gui

    downloads = {
        "test-catalog/test_catalog/flat": EntryMetadata(path="flat", size=1),
        "elsewhere": EntryMetadata(path="elsewhere", metadata={"id": "x.gone"}),
    }
    full = catalog.with_downloads(downloads)
    statuses = {e.id: gui.status(e, downloads) for e in full}
    assert statuses == {
        "test_catalog.flat": gui.DOWNLOADED,
        "test_catalog.family": gui.NOT_DOWNLOADED,
        "test_catalog.presets": gui.NOT_DOWNLOADED,
        "x.gone": gui.UNAVAILABLE,
    }
    # The status is not a search term
    assert gui.make_query("", {("status", gui.DOWNLOADED)}) == []

    by_id = {row["id"]: row for row in gui.rows(full, downloads)}
    assert by_id["x.gone"]["status"] == gui.UNAVAILABLE
    assert by_id["test_catalog.flat"]["status"] == ""


def test_cli_orphans(context, repository, monkeypatch):
    """Folders registered from another environment are marked (even inside
    an orphan folder)"""
    import shutil

    from click.testing import CliRunner

    from datamaestro import cleaner
    from datamaestro.__main__ import cli

    # test_catalog.flat has files: the other folders of test_catalog are orphans
    monkeypatch.setattr(repository.datasets[0], "has_files", lambda: True)
    base = context.datapath / "test-catalog"
    for name in ("old/gone", "junk"):
        (base / "test_catalog" / name).mkdir(parents=True)
        (base / "test_catalog" / name / "data.txt").write_text("x")
    cleaner.registry(context).register_entry(
        cleaner.PART,
        "test-catalog/test_catalog/old/gone",
        metadata={"id": "test_catalog.gone", "repository_version": "0.9"},
    )

    try:
        monkeypatch.setattr(Context, "INSTANCE", None)
        result = CliRunner().invoke(cli, ["--data", str(context._path), "orphans"])
        assert result.exit_code == 0, result.output
        lines = sorted(result.stdout.splitlines())
        assert lines == [
            f"{base}/test_catalog/junk",
            f"{base}/test_catalog/old/gone  [registered: test_catalog.gone, version 0.9]",
        ]
    finally:
        cleaner.registry(context).remove(
            cleaner.PART, "test-catalog/test_catalog/old/gone"
        )
        shutil.rmtree(base)
