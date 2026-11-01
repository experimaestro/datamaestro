"""Tests for the ds-cache-cleaner registration of downloaded datasets"""

import shutil
import uuid

import pytest

from datamaestro import cleaner
from datamaestro.context import Context

from .conftest import MyRepository
from .test_resource import DummyFileResource, SimpleDataset


class Dataset(SimpleDataset):
    def __init__(self, repository, datapath, ds_id):
        super().__init__(repository, datapath)
        self.id = ds_id
        self.name = "Some test data"
        self.url = "http://example.com/data"
        self.tags = {"b", "a"}


@pytest.fixture
def make_dataset(context):
    """Creates datasets stored in the context data folder

    The context (and so the registry) is shared by all the tests: datasets get
    unique paths
    """
    repository = MyRepository(context)
    created = []

    def make(files=("data.txt",)):
        key = uuid.uuid4().hex[:8]
        ds = Dataset(repository, context.datapath / "cleaner" / key, f"test.{key}")
        for name in files:
            DummyFileResource(name).bind(name, ds)
        created.append(ds)
        return ds

    yield make

    for ds in created:
        shutil.rmtree(ds.datapath, ignore_errors=True)


def _entry(ds):
    return cleaner.registry(ds.context).get_entry(cleaner.PART, cleaner.entry_path(ds))


def test_download_registers(make_dataset):
    ds = make_dataset()
    assert ds.download()

    entry = _entry(ds)
    assert entry is not None
    assert entry.path == f"cleaner/{ds.datapath.name}"
    assert entry.description == f"{ds.id}: Some test data"
    assert entry.size > 0
    assert entry.metadata == {
        "id": ds.id,
        "repository": "test-simple",
        "tags": ["a", "b"],
        "url": "http://example.com/data",
        "module": __name__,
    }


def test_download_again_only_touches(make_dataset):
    ds = make_dataset()
    assert ds.download()
    first = _entry(ds)

    # Nothing is downloaded: the size is not recomputed
    cleaner.registry(ds.context).update_size(cleaner.PART, first.path, 1)
    assert ds.download()

    entry = _entry(ds)
    assert entry.size == 1
    assert entry.created == first.created
    assert entry.last_access > first.last_access


def test_outside_datapath_not_registered(context, tmp_path):
    ds = Dataset(MyRepository(context), tmp_path / "elsewhere", "test.elsewhere")
    DummyFileResource("data.txt").bind("data.txt", ds)
    assert ds.download()
    assert cleaner.entry_path(ds) is None


def test_registration_errors_are_ignored(make_dataset, monkeypatch):
    def fail(context):
        raise OSError("read-only")

    monkeypatch.setattr(cleaner, "registry", fail)
    assert make_dataset().download()


def test_sync(make_dataset, monkeypatch):
    on_disk, missing, deleted = make_dataset(), make_dataset(), make_dataset()
    assert deleted.download()

    _without_registration(monkeypatch, on_disk)

    shutil.rmtree(deleted.datapath)
    datasets = [on_disk, missing, deleted]
    monkeypatch.setattr(Context, "datasets", lambda self: iter(datasets))

    context = on_disk.context
    report = cleaner.sync(context, dry_run=True)
    assert report.added == [cleaner.entry_path(on_disk)]
    assert cleaner.entry_path(deleted) in report.removed
    assert _entry(on_disk) is None

    cleaner.sync(context)
    assert _entry(on_disk) is not None
    assert _entry(missing) is None
    assert _entry(deleted) is None

    assert cleaner.sync(context).added == []


def _without_registration(monkeypatch, ds):
    """Downloads a dataset as a datamaestro version without registration"""
    with monkeypatch.context() as m:
        m.setattr(cleaner, "register", lambda *args, **kwargs: None)
        assert ds.download()
    assert _entry(ds) is None


def test_first_registration_syncs(make_dataset, monkeypatch):
    old, new = make_dataset(), make_dataset()
    _without_registration(monkeypatch, old)

    context = old.context
    marker = context.datapath / "ds-cache-cleaner" / cleaner.SYNC_MARKER
    marker.unlink(missing_ok=True)
    monkeypatch.setattr(Context, "datasets", lambda self: iter([old, new]))

    assert new.download()
    assert _entry(old) is not None
    assert _entry(new) is not None
    assert marker.exists()


def test_sync_only_once(make_dataset, monkeypatch):
    old, new = make_dataset(), make_dataset()
    assert make_dataset().download()  # makes sure the first sync is done
    _without_registration(monkeypatch, old)
    monkeypatch.setattr(Context, "datasets", lambda self: iter([old, new]))

    assert new.download()
    assert _entry(old) is None


def test_new_repository_syncs(make_dataset, monkeypatch):
    """A repository that was not installed during the first sync gets synced"""
    import json

    old, new = make_dataset(), make_dataset()
    _without_registration(monkeypatch, old)
    monkeypatch.setattr(Context, "datasets", lambda self: iter([old, new]))

    context = old.context
    marker = context.datapath / "ds-cache-cleaner" / cleaner.SYNC_MARKER
    marker.write_text(json.dumps(["other"]))
    monkeypatch.setattr(Context, "repositories", lambda self: iter([old.repository]))

    assert new.download()
    assert _entry(old) is not None
    assert json.loads(marker.read_text()) == ["other", "test-simple"]


def test_old_marker_syncs_again(make_dataset, monkeypatch):
    old, new = make_dataset(), make_dataset()
    _without_registration(monkeypatch, old)
    monkeypatch.setattr(Context, "datasets", lambda self: iter([old, new]))

    marker = old.context.datapath / "ds-cache-cleaner" / cleaner.SYNC_MARKER
    marker.write_text("")
    monkeypatch.setattr(Context, "repositories", lambda self: iter([old.repository]))

    assert new.download()
    assert _entry(old) is not None


def test_repository_version(make_dataset, monkeypatch):
    """The entry records the repository version that last used it"""
    ds = make_dataset()
    monkeypatch.setattr(MyRepository, "version", classmethod(lambda cls: "1.0"))
    assert ds.download()
    first = _entry(ds)
    assert first.metadata["repository_version"] == "1.0"

    # Used from an environment with another version: metadata is updated,
    # but the size is not recomputed
    cleaner.registry(ds.context).update_size(cleaner.PART, first.path, 1)
    monkeypatch.setattr(MyRepository, "version", classmethod(lambda cls: "2.0"))
    assert ds.download()

    entry = _entry(ds)
    assert entry.metadata["repository_version"] == "2.0"
    assert entry.size == 1
    assert entry.created == first.created


def test_new_repository_version_syncs(make_dataset, monkeypatch):
    """A repository version that was never synced gets synced"""
    import json

    old, new = make_dataset(), make_dataset()
    _without_registration(monkeypatch, old)
    monkeypatch.setattr(Context, "datasets", lambda self: iter([old, new]))
    monkeypatch.setattr(Context, "repositories", lambda self: iter([old.repository]))
    monkeypatch.setattr(MyRepository, "version", classmethod(lambda cls: "2.0"))

    marker = old.context.datapath / "ds-cache-cleaner" / cleaner.SYNC_MARKER
    marker.write_text(json.dumps(["test-simple==1.0"]))

    assert new.download()
    assert _entry(old) is not None
    assert json.loads(marker.read_text()) == ["test-simple==1.0", "test-simple==2.0"]
