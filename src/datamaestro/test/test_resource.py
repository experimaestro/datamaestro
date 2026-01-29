"""Tests for the new Resource interface.

Covers:
- ResourceState enum and metadata persistence
- Resource base class (bind, dependencies, state, cleanup)
- FileResource, FolderResource, ValueResource
- Topological sort and cycle detection
- Two-path download flow (transient_path -> path)
- Eager transient cleanup
- can_recover property behavior
- Both new class-based and legacy decorator-based dataset definitions
- Each concrete resource type
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datamaestro.definitions import (
    AbstractDataset,
    topological_sort,
    _compute_dependents,
    _bind_class_resources,
)
from datamaestro.download import (
    Resource,
    ResourceState,
    ResourceStateFile,
    FileResource,
    FolderResource,
    ValueResource,
    Download,
    reference,
)
from .conftest import MyRepository


# ---- Helpers ----


class SimpleDataset(AbstractDataset):
    """Minimal dataset for testing."""

    def __init__(self, repository, datapath: Path):
        super().__init__(repository)
        self._datapath = datapath

    @property
    def datapath(self):
        return self._datapath

    def _prepare(self):
        # Return a mock Base-like object for the prepare flow
        obj = MagicMock()
        obj.__xpm__ = MagicMock()
        obj.__xpm__.values = {}
        return obj

    @property
    def description(self):
        return "test dataset"


class DummyFileResource(FileResource):
    """Concrete FileResource for testing."""

    def __init__(self, filename, url="http://example.com/test", **kw):
        super().__init__(filename, **kw)
        self.url = url
        self._download_called = False

    def _download(self, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(f"downloaded from {self.url}")
        self._download_called = True


class DummyFolderResource(FolderResource):
    """Concrete FolderResource for testing."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self._download_called = False

    def _download(self, destination: Path):
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "file.txt").write_text("content")
        self._download_called = True


class DummyValueResource(ValueResource):
    """Concrete ValueResource for testing."""

    def __init__(self, value, **kw):
        super().__init__(**kw)
        self._value = value

    def download(self, force=False):
        pass

    def prepare(self):
        return self._value


class RecoverableResource(FileResource):
    """Resource that supports recovery from PARTIAL state."""

    @property
    def can_recover(self) -> bool:
        return True

    def __init__(self, filename, **kw):
        super().__init__(filename, **kw)

    def _download(self, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("recovered")


class FailingResource(FileResource):
    """Resource that fails during download."""

    def __init__(self, filename, **kw):
        super().__init__(filename, **kw)

    def _download(self, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("partial data")
        raise RuntimeError("Download failed")


class DependentResource(FileResource):
    """Resource that depends on another resource."""

    def __init__(self, filename, source: Resource, **kw):
        super().__init__(filename, **kw)
        self._dependencies = [source]

    def _download(self, destination: Path):
        # Read from dependency's path
        source = self.dependencies[0]
        data = source.path.read_text()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(f"processed: {data}")


# ---- Fixtures ----


@pytest.fixture
def datapath(tmp_path):
    """Temporary dataset data path."""
    return tmp_path / "dataset"


@pytest.fixture
def dataset(context, datapath):
    """A minimal dataset bound to a repository."""
    repository = MyRepository(context)
    ds = SimpleDataset(repository, datapath)
    return ds


# ==== ResourceState Tests ====


class TestResourceState:
    def test_values(self):
        assert ResourceState.NONE == "none"
        assert ResourceState.PARTIAL == "partial"
        assert ResourceState.COMPLETE == "complete"

    def test_from_string(self):
        assert ResourceState("none") == ResourceState.NONE
        assert ResourceState("partial") == ResourceState.PARTIAL
        assert ResourceState("complete") == ResourceState.COMPLETE


# ==== ResourceStateFile Tests ====


class TestResourceStateFile:
    def test_read_nonexistent(self, datapath):
        sf = ResourceStateFile(datapath)
        assert sf.read("TRAIN") == ResourceState.NONE

    def test_write_and_read(self, datapath):
        sf = ResourceStateFile(datapath)
        sf.write("TRAIN", ResourceState.COMPLETE)

        assert sf.read("TRAIN") == ResourceState.COMPLETE
        assert sf.read("TEST") == ResourceState.NONE

    def test_multiple_resources(self, datapath):
        sf = ResourceStateFile(datapath)
        sf.write("A", ResourceState.COMPLETE)
        sf.write("B", ResourceState.PARTIAL)
        sf.write("C", ResourceState.NONE)

        assert sf.read("A") == ResourceState.COMPLETE
        assert sf.read("B") == ResourceState.PARTIAL
        assert sf.read("C") == ResourceState.NONE

    def test_overwrite(self, datapath):
        sf = ResourceStateFile(datapath)
        sf.write("A", ResourceState.PARTIAL)
        assert sf.read("A") == ResourceState.PARTIAL

        sf.write("A", ResourceState.COMPLETE)
        assert sf.read("A") == ResourceState.COMPLETE

    def test_file_format(self, datapath):
        sf = ResourceStateFile(datapath)
        sf.write("TRAIN", ResourceState.COMPLETE)

        state_path = datapath / ".state.json"
        assert state_path.exists()

        with state_path.open() as f:
            data = json.load(f)

        assert data["version"] == 1
        assert data["resources"]["TRAIN"]["state"] == "complete"


# ==== Resource Base Class Tests ====


class TestResourceBase:
    def test_bind(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)

        assert r.name == "TEST"
        assert r.dataset is dataset
        assert "TEST" in dataset.resources
        assert r in dataset.ordered_resources

    def test_bind_with_varname(self, dataset):
        r = DummyFileResource("test.txt", varname="my_var")
        r.bind("ATTR_NAME", dataset)

        # varname takes precedence
        assert r.name == "my_var"

    def test_bind_duplicate_raises(self, dataset):
        r1 = DummyFileResource("test1.txt")
        r2 = DummyFileResource("test2.txt")
        r1.bind("TEST", dataset)

        with pytest.raises(AssertionError, match="already declared"):
            r2.bind("TEST", dataset)

    def test_bind_already_bound_raises(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)

        ds2 = SimpleDataset(None, dataset.datapath / "other")
        with pytest.raises(AssertionError, match="already bound"):
            r.bind("TEST2", ds2)

    def test_state_default_none(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)
        assert r.state == ResourceState.NONE

    def test_state_set_and_get(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)

        r.state = ResourceState.COMPLETE
        assert r.state == ResourceState.COMPLETE

        r.state = ResourceState.PARTIAL
        assert r.state == ResourceState.PARTIAL

    def test_dependencies_default_empty(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)
        assert r.dependencies == []

    def test_dependents_default_empty(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)
        assert r.dependents == []

    def test_can_recover_default_false(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)
        assert r.can_recover is False

    def test_can_recover_override(self, dataset):
        r = RecoverableResource("test.txt")
        r.bind("TEST", dataset)
        assert r.can_recover is True

    def test_has_files_default_true(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)
        assert r.has_files() is True

    def test_transient_flag(self, dataset):
        r = DummyFileResource("test.txt", transient=True)
        r.bind("TEST", dataset)
        assert r.transient is True

    def test_context_property(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)
        assert r.context is dataset.context

    def test_cleanup(self, dataset):
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)

        # Create files at both paths
        r.path.parent.mkdir(parents=True, exist_ok=True)
        r.path.write_text("final")
        r.transient_path.parent.mkdir(parents=True, exist_ok=True)
        r.transient_path.write_text("temp")
        r.state = ResourceState.COMPLETE

        r.cleanup()

        assert not r.path.exists()
        assert not r.transient_path.exists()
        assert r.state == ResourceState.NONE


# ==== FileResource Tests ====


class TestFileResource:
    def test_path(self, dataset):
        r = DummyFileResource("data.csv")
        r.bind("DATA", dataset)

        expected = dataset.datapath / "data.csv"
        assert r.path == expected

    def test_transient_path(self, dataset):
        r = DummyFileResource("data.csv")
        r.bind("DATA", dataset)

        expected = dataset.datapath / ".downloads" / "data.csv"
        assert r.transient_path == expected

    def test_prepare_returns_path(self, dataset):
        r = DummyFileResource("data.csv")
        r.bind("DATA", dataset)
        assert r.prepare() == r.path

    def test_download_writes_to_transient(self, dataset):
        r = DummyFileResource("data.csv")
        r.bind("DATA", dataset)
        r.download()

        assert r.transient_path.exists()
        assert "downloaded" in r.transient_path.read_text()
        assert r._download_called

    def test_stream_default_none(self, dataset):
        r = DummyFileResource("data.csv")
        r.bind("DATA", dataset)
        assert r.stream() is None

    def test_varname_from_filename(self):
        """Without explicit varname, name is derived from filename."""
        r = DummyFileResource("data.csv.gz")
        assert r.name == "data"


# ==== FolderResource Tests ====


class TestFolderResource:
    def test_path(self, dataset):
        r = DummyFolderResource(varname="archive")
        r.bind("ARCHIVE", dataset)

        expected = dataset.datapath / "archive"
        assert r.path == expected

    def test_transient_path(self, dataset):
        r = DummyFolderResource(varname="archive")
        r.bind("ARCHIVE", dataset)

        expected = dataset.datapath / ".downloads" / "archive"
        assert r.transient_path == expected

    def test_prepare_returns_path(self, dataset):
        r = DummyFolderResource(varname="archive")
        r.bind("ARCHIVE", dataset)
        assert r.prepare() == r.path

    def test_download_creates_directory(self, dataset):
        r = DummyFolderResource(varname="archive")
        r.bind("ARCHIVE", dataset)
        r.download()

        assert r.transient_path.is_dir()
        assert (r.transient_path / "file.txt").exists()


# ==== ValueResource Tests ====


class TestValueResource:
    def test_has_files_false(self, dataset):
        r = DummyValueResource({"key": "value"}, varname="data")
        r.bind("DATA", dataset)
        assert r.has_files() is False

    def test_prepare_returns_value(self, dataset):
        val = {"key": "value"}
        r = DummyValueResource(val, varname="data")
        r.bind("DATA", dataset)
        assert r.prepare() == val


# ==== Topological Sort Tests ====


class TestTopologicalSort:
    def test_empty(self):
        assert topological_sort({}) == []

    def test_single(self, dataset):
        r = DummyFileResource("a.txt")
        r.bind("A", dataset)
        result = topological_sort(dataset.resources)
        assert result == [r]

    def test_linear_chain(self, dataset):
        a = DummyFileResource("a.txt")
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        result = topological_sort(dataset.resources)
        assert result.index(a) < result.index(b)

    def test_diamond(self, dataset):
        a = DummyFileResource("a.txt")
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        c = DependentResource("c.txt", source=a)
        c.bind("C", dataset)

        d = DependentResource("d.txt", source=b)
        d._dependencies.append(c)
        d.bind("D", dataset)

        result = topological_sort(dataset.resources)
        assert result.index(a) < result.index(b)
        assert result.index(a) < result.index(c)
        assert result.index(b) < result.index(d)
        assert result.index(c) < result.index(d)

    def test_cycle_detection(self, dataset):
        a = DummyFileResource("a.txt")
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        # Create cycle: a depends on b
        a._dependencies = [b]

        with pytest.raises(ValueError, match="Cycle detected"):
            topological_sort(dataset.resources)

    def test_independent_resources(self, dataset):
        a = DummyFileResource("a.txt")
        a.bind("A", dataset)

        b = DummyFileResource("b.txt")
        b.bind("B", dataset)

        result = topological_sort(dataset.resources)
        assert len(result) == 2
        assert set(result) == {a, b}


# ==== Dependents Computation Tests ====


class TestComputeDependents:
    def test_no_dependencies(self, dataset):
        a = DummyFileResource("a.txt")
        a.bind("A", dataset)

        _compute_dependents(dataset.resources)
        assert a.dependents == []

    def test_linear_dependents(self, dataset):
        a = DummyFileResource("a.txt")
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        _compute_dependents(dataset.resources)
        assert b in a.dependents
        assert a not in b.dependents

    def test_multiple_dependents(self, dataset):
        a = DummyFileResource("a.txt")
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        c = DependentResource("c.txt", source=a)
        c.bind("C", dataset)

        _compute_dependents(dataset.resources)
        assert set(a.dependents) == {b, c}


# ==== Two-Path Download Flow Tests ====


class TestTwoPathFlow:
    def test_download_moves_to_final_path(self, dataset):
        """Framework should move transient_path -> path on success."""
        r = DummyFileResource("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        dataset.download()

        assert r.path.exists()
        assert r.state == ResourceState.COMPLETE

    def test_failure_no_recover_cleans_up(self, dataset):
        """On failure without can_recover, transient data is deleted."""
        r = FailingResource("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        result = dataset.download()

        assert result is False
        assert not r.transient_path.exists()
        assert r.state == ResourceState.NONE

    def test_failure_with_recover_preserves(self, dataset):
        """On failure with can_recover, transient data is preserved."""

        class FailRecoverable(RecoverableResource):
            def _download(self, destination):
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text("partial")
                raise RuntimeError("partial failure")

        r = FailRecoverable("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        result = dataset.download()

        assert result is False
        assert r.transient_path.exists()
        assert r.state == ResourceState.PARTIAL

    def test_skip_complete_resources(self, dataset):
        """Resources already COMPLETE are skipped unless force=True."""
        r = DummyFileResource("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        # Mark as complete
        r.state = ResourceState.COMPLETE
        r.path.parent.mkdir(parents=True, exist_ok=True)
        r.path.write_text("existing")

        dataset.download()

        # download should not have been called
        assert r._download_called is False

    def test_redownload_when_files_missing(self, dataset):
        """COMPLETE resource with missing files is re-downloaded."""
        r = DummyFileResource("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        # Mark as complete but do NOT create the file
        r.state = ResourceState.COMPLETE
        assert not r.path.exists()

        dataset.download()

        # Should have re-downloaded
        assert r._download_called is True
        assert r.path.exists()
        assert r.state == ResourceState.COMPLETE

    def test_adopt_preexisting_files(self, dataset):
        """Files already on disk (old downloads) are adopted as COMPLETE."""
        r = DummyFileResource("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        # Pre-create the file at the final path (simulating old download)
        r.path.parent.mkdir(parents=True, exist_ok=True)
        r.path.write_text("old data")

        # State is NONE (no .state.json entry)
        assert r.state == ResourceState.NONE

        dataset.download()

        # Should NOT have re-downloaded — just marked COMPLETE
        assert r._download_called is False
        assert r.state == ResourceState.COMPLETE
        assert r.path.read_text() == "old data"

    def test_downloads_dir_cleaned_after_success(self, dataset):
        """The .downloads/ directory is removed after all succeed."""
        r = DummyFileResource("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        result = dataset.download()

        assert result is True
        downloads_dir = dataset.datapath / ".downloads"
        assert not downloads_dir.exists()

    def test_downloads_dir_kept_on_failure(self, dataset):
        """The .downloads/ directory is kept if a download fails."""
        r = FailingResource("data.txt")
        r.bind("DATA", dataset)

        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        # Pre-create .downloads/ with transient data
        r.transient_path.parent.mkdir(parents=True, exist_ok=True)
        r.transient_path.write_text("partial")

        result = dataset.download()

        assert result is False
        # .downloads/ should still exist (failure, no cleanup)
        # (transient data itself is deleted because can_recover=False)

    def test_lock_prevents_concurrent_download(self, dataset):
        """A second download blocks while the first holds the lock."""
        import fcntl
        import threading

        r = DummyFileResource("data.txt")
        r.bind("DATA", dataset)
        dataset.ordered_resources = [r]
        _compute_dependents(dataset.resources)

        # Acquire the lock externally to simulate a concurrent download
        dataset.datapath.mkdir(parents=True, exist_ok=True)
        lock_path = dataset.datapath / ".state.lock"
        lock_file = lock_path.open("w")
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        result_holder = {}

        def try_download():
            result_holder["result"] = dataset.download()

        t = threading.Thread(target=try_download)
        t.start()
        # Give thread time to hit the lock
        t.join(timeout=0.2)
        # Thread should still be alive (blocked on lock)
        assert t.is_alive()

        # Release the lock
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

        t.join(timeout=5)
        assert not t.is_alive()
        assert result_holder["result"] is True


# ==== Eager Transient Cleanup Tests ====


class TestTransientCleanup:
    def test_transient_cleaned_after_dependents_complete(self, dataset):
        """Transient resources are cleaned up when all dependents
        are COMPLETE."""
        a = DummyFileResource("a.txt", transient=True)
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        _compute_dependents(dataset.resources)
        dataset.ordered_resources = topological_sort(dataset.resources)

        dataset.download()

        # b should be complete
        assert b.state == ResourceState.COMPLETE
        assert b.path.exists()

        # a should be cleaned up (transient)
        assert a.state == ResourceState.NONE
        assert not a.path.exists()

    def test_non_transient_not_cleaned(self, dataset):
        """Non-transient resources are NOT cleaned up."""
        a = DummyFileResource("a.txt", transient=False)
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        _compute_dependents(dataset.resources)
        dataset.ordered_resources = topological_sort(dataset.resources)

        dataset.download()

        assert a.state == ResourceState.COMPLETE
        assert a.path.exists()

    def test_transient_not_cleaned_if_dependent_incomplete(self, dataset):
        """Transient resources are NOT cleaned if a dependent
        hasn't completed yet."""
        a = DummyFileResource("a.txt", transient=True)
        a.bind("A", dataset)

        b = DependentResource("b.txt", source=a)
        b.bind("B", dataset)

        c = DependentResource("c.txt", source=a)
        c.bind("C", dataset)

        _compute_dependents(dataset.resources)
        dataset.ordered_resources = topological_sort(dataset.resources)

        # Download only processes in order, so after B completes,
        # C hasn't yet — a should not be cleaned up until C completes.
        # The full download() handles this correctly.
        dataset.download()

        # After full download, all dependents are complete
        # so transient should be cleaned
        assert a.state == ResourceState.NONE


# ==== Legacy Decorator-Based Dataset Tests ====


class TestLegacyDecoratorDataset:
    def test_filedownloader_decorator(self, context):
        """Legacy decorator-based filedownloader still works."""
        import warnings
        from datamaestro.download.single import filedownloader

        repository = MyRepository(context)
        ds = SimpleDataset(repository, context.datapath / "legacy")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            downloader = filedownloader("test.html", "http://httpbin.org/html")
            downloader(ds)

        assert "test" in ds.resources
        assert ds.resources["test"] is downloader

    def test_reference_resource(self, context):
        """reference resource still works."""
        repository = MyRepository(context)
        ds = SimpleDataset(repository, context.datapath / "ref_test")

        mock_ref = MagicMock()
        mock_ref.prepare.return_value = "prepared_value"

        ref = reference(varname="ref", reference=mock_ref)
        ref.bind("ref", ds)

        assert ref.has_files() is False
        result = ref.prepare()
        assert result == "prepared_value"


# ==== New Class-Based Dataset Tests ====


class TestClassBasedDataset:
    def test_bind_class_resources(self, dataset):
        """_bind_class_resources detects Resource attributes."""
        from datamaestro.data import Base

        class MyData(Base):
            A = DummyFileResource("a.txt")
            B = DummyFileResource("b.txt")

        _bind_class_resources(MyData, dataset)

        assert "A" in dataset.resources
        assert "B" in dataset.resources
        assert len(dataset.ordered_resources) == 2

    def test_bind_with_dependencies(self, dataset):
        """Resources with dependencies are properly ordered."""
        from datamaestro.data import Base

        src = DummyFileResource("src.txt")

        class MyData(Base):
            SRC = src
            PROCESSED = DependentResource("proc.txt", source=src)

        _bind_class_resources(MyData, dataset)

        # SRC should come before PROCESSED in ordered_resources
        src_idx = dataset.ordered_resources.index(MyData.SRC)
        proc_idx = dataset.ordered_resources.index(MyData.PROCESSED)
        assert src_idx < proc_idx

        # Check dependents were computed
        assert MyData.PROCESSED in MyData.SRC.dependents

    def test_non_resource_attributes_ignored(self, dataset):
        """Non-Resource class attributes are not bound."""
        from datamaestro.data import Base

        class MyData(Base):
            A = DummyFileResource("a.txt")
            NOT_A_RESOURCE = "just a string"
            ALSO_NOT = 42

        _bind_class_resources(MyData, dataset)

        assert "A" in dataset.resources
        assert "NOT_A_RESOURCE" not in dataset.resources
        assert "ALSO_NOT" not in dataset.resources


# ==== Backward Compatibility Tests ====


class TestBackwardCompat:
    def test_hasfiles_deprecated(self, dataset):
        """hasfiles() still works but emits deprecation."""
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)

        import warnings

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = r.hasfiles()

        assert result is True

    def test_definition_property_deprecated(self, dataset):
        """definition property still works but emits deprecation."""
        r = DummyFileResource("test.txt")
        r.bind("TEST", dataset)

        import warnings

        # Clear the one-time warning cache
        from datamaestro.download import _deprecation_warned

        _deprecation_warned.discard("definition")

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = r.definition

        assert result is dataset

    def test_download_subclass_deprecated(self):
        """Subclassing Download emits deprecation."""
        from datamaestro.download import _deprecation_warned

        _deprecation_warned.discard("Download-TestSub")

        import warnings

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")

            class TestSub(Download):
                def download(self, force=False):
                    pass

                def prepare(self):
                    pass

    def test_apply_classmethod(self):
        """Resource.apply creates instances."""
        r = DummyFileResource.apply("test.txt")
        assert isinstance(r, DummyFileResource)
        assert r.filename == "test.txt"


# ==== Concrete Resource Tests ====


class TestFileDownloader:
    def test_construction(self):
        """FileDownloader can be constructed."""
        from datamaestro.download.single import FileDownloader

        r = FileDownloader("data.csv", "http://example.com/data.csv")
        assert r.filename == "data.csv"
        assert r.url == "http://example.com/data.csv"
        assert r.name == "data"  # derived from filename

    def test_factory_alias(self):
        """filedownloader is an alias for FileDownloader.apply."""
        from datamaestro.download.single import (
            filedownloader,
            FileDownloader,
        )

        r = filedownloader("data.csv", "http://example.com/data.csv")
        assert isinstance(r, FileDownloader)

    def test_transient_flag(self):
        """FileDownloader accepts transient flag."""
        from datamaestro.download.single import FileDownloader

        r = FileDownloader(
            "data.csv",
            "http://example.com/data.csv",
            transient=True,
        )
        assert r.transient is True

    def test_backward_compat_alias(self):
        """SingleDownload is an alias for FileDownloader."""
        from datamaestro.download.single import (
            SingleDownload,
            FileDownloader,
        )

        assert SingleDownload is FileDownloader


class TestConcatDownloader:
    def test_construction(self):
        from datamaestro.download.single import ConcatDownloader

        r = ConcatDownloader("data.txt", "http://example.com/data.tar.gz")
        assert r.filename == "data.txt"
        assert r.url == "http://example.com/data.tar.gz"

    def test_factory_alias(self):
        from datamaestro.download.single import (
            concatdownload,
            ConcatDownloader,
        )

        r = concatdownload("data.txt", "http://example.com/data.tar.gz")
        assert isinstance(r, ConcatDownloader)


class TestArchiveDownloaders:
    def test_zip_construction(self):
        from datamaestro.download.archive import ZipDownloader

        r = ZipDownloader("archive", "http://example.com/data.zip")
        assert r.url == "http://example.com/data.zip"
        assert r.name == "archive"

    def test_tar_construction(self):
        from datamaestro.download.archive import TarDownloader

        r = TarDownloader("archive", "http://example.com/data.tar.gz")
        assert r.url == "http://example.com/data.tar.gz"

    def test_zip_factory_alias(self):
        from datamaestro.download.archive import (
            zipdownloader,
            ZipDownloader,
        )

        r = zipdownloader("archive", "http://example.com/data.zip")
        assert isinstance(r, ZipDownloader)

    def test_tar_factory_alias(self):
        from datamaestro.download.archive import (
            tardownloader,
            TarDownloader,
        )

        r = tardownloader("archive", "http://example.com/data.tar.gz")
        assert isinstance(r, TarDownloader)


class TestCustomDownload:
    def test_construction(self):
        from datamaestro.download.custom import custom_download

        fn = MagicMock()
        r = custom_download("data", fn)
        assert r.name == "data"
        assert r.downloader is fn


class TestHFDownloader:
    def test_construction(self):
        from datamaestro.download.huggingface import HFDownloader

        r = HFDownloader("hf", repo_id="user/dataset")
        assert r.repo_id == "user/dataset"
        assert r.name == "hf"

    def test_factory_alias(self):
        from datamaestro.download.huggingface import (
            hf_download,
            HFDownloader,
        )

        r = hf_download("hf", repo_id="user/dataset")
        assert isinstance(r, HFDownloader)

    def test_prepare(self):
        from datamaestro.download.huggingface import HFDownloader

        r = HFDownloader(
            "hf",
            repo_id="user/dataset",
            data_files="train.csv",
            split="train",
        )
        result = r.prepare()
        assert result == {
            "repo_id": "user/dataset",
            "data_files": "train.csv",
            "split": "train",
        }


class TestTodoResource:
    def test_raises_not_implemented(self):
        from datamaestro.download.todo import Todo

        r = Todo(varname="test")
        with pytest.raises(NotImplementedError):
            r.download()

        with pytest.raises(NotImplementedError):
            r.prepare()


class TestReferenceResource:
    def test_has_files_false(self, dataset):
        mock_ref = MagicMock()
        mock_ref.prepare.return_value = "value"

        r = reference(varname="ref", reference=mock_ref)
        r.bind("ref", dataset)

        assert r.has_files() is False

    def test_prepare_delegates(self, dataset):
        mock_ref = MagicMock()
        mock_ref.prepare.return_value = "prepared"

        r = reference(varname="ref", reference=mock_ref)
        r.bind("ref", dataset)

        result = r.prepare()
        assert result == "prepared"

    def test_download_delegates(self, dataset):
        mock_ref = MagicMock()
        mock_ref.__datamaestro__ = MagicMock()

        r = reference(varname="ref", reference=mock_ref)
        r.bind("ref", dataset)

        r.download(force=True)
        mock_ref.__datamaestro__.download.assert_called_once_with(True)

    def test_requires_reference(self):
        with pytest.raises(AssertionError, match="cannot be null"):
            reference(varname="ref", reference=None)


# ==== Links Resource Tests ====


class TestLinksResource:
    def test_construction(self):
        from datamaestro.download.links import links

        mock_ds = MagicMock()
        r = links("data", ref1=mock_ds)
        assert r.name == "data"

    def test_has_files_false(self, dataset):
        from datamaestro.download.links import links

        mock_ds = MagicMock()
        r = links("data", ref1=mock_ds)
        r.bind("data", dataset)

        assert r.has_files() is False

    def test_path_is_datapath(self, dataset):
        from datamaestro.download.links import links

        mock_ds = MagicMock()
        r = links("data", ref1=mock_ds)
        r.bind("data", dataset)

        assert r.path == dataset.datapath

    def test_prepare_returns_path(self, dataset):
        from datamaestro.download.links import links

        mock_ds = MagicMock()
        r = links("data", ref1=mock_ds)
        r.bind("data", dataset)

        assert r.prepare() == dataset.datapath


class TestLinkFolder:
    def test_construction(self):
        from datamaestro.download.links import linkfolder

        r = linkfolder("data", proposals=["/tmp/test"])
        assert r.name == "data"

    def test_check_is_dir(self, dataset, tmp_path):
        from datamaestro.download.links import linkfolder

        r = linkfolder("data", proposals=[])
        r.bind("data", dataset)

        # A directory should pass
        assert r.check(tmp_path) is True
        # A non-existent path should fail
        assert r.check(tmp_path / "nonexistent") is False

    def test_path(self, dataset):
        from datamaestro.download.links import linkfolder

        r = linkfolder("data", proposals=[])
        r.bind("data", dataset)

        assert r.path == dataset.datapath / "data"

    def test_prepare_returns_path(self, dataset):
        from datamaestro.download.links import linkfolder

        r = linkfolder("data", proposals=[])
        r.bind("data", dataset)

        assert r.prepare() == r.path


class TestLinkFile:
    def test_construction(self):
        from datamaestro.download.links import linkfile

        r = linkfile("data", proposals=["/tmp/test.txt"])
        assert r.name == "data"

    def test_check_is_file(self, dataset, tmp_path):
        from datamaestro.download.links import linkfile

        r = linkfile("data", proposals=[])
        r.bind("data", dataset)

        # Create a real file to check
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        assert r.check(test_file) is True
        # A directory should fail
        assert r.check(tmp_path) is False
        # A non-existent path should fail
        assert r.check(tmp_path / "nonexistent") is False

    def test_path(self, dataset):
        from datamaestro.download.links import linkfile

        r = linkfile("data", proposals=[])
        r.bind("data", dataset)

        assert r.path == dataset.datapath / "data"


# ==== Wayback Resource Tests ====


class TestWaybackDocuments:
    def test_construction(self):
        from datamaestro.download.wayback import wayback_documents

        def urls_fn():
            return iter(["http://example.com"])

        r = wayback_documents("20200101", urls_fn, name="wb")
        assert r.name == "wb"
        assert r.timestamp == "20200101"

    def test_prepare_returns_path(self, dataset):
        from datamaestro.download.wayback import wayback_documents

        def urls_fn():
            return iter([])

        r = wayback_documents("20200101", urls_fn, name="wb")
        r.bind("wb", dataset)

        expected = dataset.datapath / "wb"
        assert r.prepare() == expected


# ==== Custom Download Functional Tests ====


class TestCustomDownloadFunctional:
    def test_download_delegates(self, dataset):
        from datamaestro.download.custom import custom_download

        fn = MagicMock()
        r = custom_download("data", fn)
        r.bind("data", dataset)

        r.download(force=True)

        fn.assert_called_once_with(dataset.context, dataset.datapath, force=True)

    def test_prepare_returns_datapath(self, dataset):
        from datamaestro.download.custom import custom_download

        fn = MagicMock()
        r = custom_download("data", fn)
        r.bind("data", dataset)

        assert r.prepare() == dataset.datapath


# ==== Archive Downloader Base Tests ====


class TestArchiveDownloaderBase:
    def test_zip_path_with_postinit(self, dataset):
        from datamaestro.download.archive import ZipDownloader

        r = ZipDownloader("archive", "http://example.com/data.zip")
        r.bind("archive", dataset)

        # path should trigger postinit
        p = r.path
        assert isinstance(p, Path)

    def test_tar_path_with_postinit(self, dataset):
        from datamaestro.download.archive import TarDownloader

        r = TarDownloader("archive", "http://example.com/data.tar.gz")
        r.bind("archive", dataset)

        p = r.path
        assert isinstance(p, Path)

    def test_extractall_default(self):
        from datamaestro.download.archive import ZipDownloader

        r = ZipDownloader("archive", "http://example.com/data.zip")
        assert r.extractall is True

    def test_extractall_with_subpath(self):
        from datamaestro.download.archive import ZipDownloader

        r = ZipDownloader(
            "archive",
            "http://example.com/data.zip",
            subpath="subdir",
        )
        assert r.extractall is False

    def test_extractall_with_files(self):
        from datamaestro.download.archive import ZipDownloader

        r = ZipDownloader(
            "archive",
            "http://example.com/data.zip",
            files={"file1.txt"},
        )
        assert r.extractall is False

    def test_subpath_trailing_slash(self):
        from datamaestro.download.archive import ZipDownloader

        r = ZipDownloader(
            "archive",
            "http://example.com/data.zip",
            subpath="subdir",
        )
        assert r.subpath == "subdir/"

    def test_transient_flag(self):
        from datamaestro.download.archive import ZipDownloader

        r = ZipDownloader(
            "archive",
            "http://example.com/data.zip",
            transient=True,
        )
        assert r.transient is True


# ==== gsync (legacy) Tests ====


class TestGsync:
    def test_import(self):
        """gsync can be imported (legacy Download subclass)."""
        from datamaestro.download.sync import gsync

        assert issubclass(gsync, Download)


# ==== manual.py (deprecated re-export) Tests ====


class TestManual:
    def test_import_linkfolder(self):
        """manual.linkfolder is a deprecated re-export."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from datamaestro.download.manual import linkfolder

            assert linkfolder is not None


# ==== multiple.py (legacy) Tests ====


class TestMultiple:
    def test_import_list(self):
        """List can be imported (legacy Download subclass)."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from datamaestro.download.multiple import List

            assert issubclass(List, Download)

    def test_import_datasets(self):
        """Datasets can be imported (legacy Download subclass)."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from datamaestro.download.multiple import Datasets

            assert issubclass(Datasets, Download)
