"""Tests for the HuggingFace adapter (datamaestro.data.huggingface +
datamaestro.download.huggingface).

Covers:
- ``load_dataset`` argument plumbing for ``name``, ``split``, ``streaming``.
- Identity: differing ``Param`` fields change the experimaestro hash;
  differing ``Meta`` fields (``streaming``, ``local_path``) do NOT.
- ``local_path`` short-circuits network access in ``HFDownloader`` and
  routes ``HuggingFaceDataset.data`` through the local mirror.
- Regression: the pre-existing bug where ``HFDownloader.download`` swallowed
  the ``split`` argument.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from datamaestro.data.huggingface import HuggingFaceDataset
from datamaestro.download.huggingface import HFDownloader


# ---- Fake `datasets` module ----------------------------------------------


@pytest.fixture
def fake_datasets(monkeypatch):
    """Inject a fake ``datasets`` module so tests don't touch the Hub."""
    fake = types.ModuleType("datasets")
    fake.load_dataset = MagicMock(return_value=MagicMock(name="FakeDataset"))
    monkeypatch.setitem(sys.modules, "datasets", fake)
    return fake


# ---- HuggingFaceDataset.data --------------------------------------------


class TestHuggingFaceDatasetData:
    def test_passes_name_split_streaming(self, fake_datasets):
        ds = HuggingFaceDataset.C(
            id="test.hf.1",
            repo_id="user/dataset",
            name="config-a",
            split="train",
            streaming=True,
        )
        _ = ds.data
        fake_datasets.load_dataset.assert_called_once_with(
            "user/dataset",
            "config-a",
            data_files=None,
            split="train",
            streaming=True,
        )

    def test_local_path_replaces_repo_id(self, fake_datasets, tmp_path):
        local = tmp_path / "mirror"
        local.mkdir()
        ds = HuggingFaceDataset.C(
            id="test.hf.2",
            repo_id="user/dataset",
            local_path=local,
        )
        _ = ds.data
        # Source is the local path, not the repo id.
        fake_datasets.load_dataset.assert_called_once()
        positional = fake_datasets.load_dataset.call_args.args
        assert positional[0] == str(local)
        assert positional[1] is None  # no name

    def test_default_args(self, fake_datasets):
        ds = HuggingFaceDataset.C(id="test.hf.3", repo_id="user/dataset")
        _ = ds.data
        fake_datasets.load_dataset.assert_called_once_with(
            "user/dataset",
            None,  # name
            data_files=None,
            split=None,
            streaming=False,
        )


# ---- Identity: Param vs Meta --------------------------------------------


class TestHuggingFaceDatasetIdentity:
    def _ident(self, ds):
        return ds.__xpm__.identifier.main

    def test_same_params_same_identity(self):
        a = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset")
        b = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset")
        assert self._ident(a) == self._ident(b)

    def test_different_name_different_identity(self):
        a = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", name="x")
        b = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", name="y")
        assert self._ident(a) != self._ident(b)

    def test_different_split_different_identity(self):
        a = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", split="train")
        b = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", split="test")
        assert self._ident(a) != self._ident(b)

    def test_streaming_meta_does_not_change_identity(self):
        """``streaming`` is Meta → changing it should NOT change the hash."""
        a = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", streaming=False)
        b = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", streaming=True)
        assert self._ident(a) == self._ident(b)

    def test_local_path_meta_does_not_change_identity(self, tmp_path):
        """``local_path`` is Meta → same logical dataset regardless of
        where the bytes come from."""
        p = tmp_path / "mirror"
        a = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset")
        b = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", local_path=p)
        assert self._ident(a) == self._ident(b)


# ---- HFDownloader --------------------------------------------------------


class TestHFDownloaderDownload:
    def test_download_passes_all_args(self, fake_datasets):
        r = HFDownloader(
            "hf",
            repo_id="user/dataset",
            name="cfg",
            data_files="train.jsonl.gz",
            split="train",
            streaming=True,
        )
        result = r.download()
        assert result is True
        fake_datasets.load_dataset.assert_called_once_with(
            "user/dataset",
            "cfg",
            data_files="train.jsonl.gz",
            split="train",
            streaming=True,
        )

    def test_download_passes_split_regression(self, fake_datasets):
        """Regression: pre-existing bug where ``split`` was accepted but
        dropped before ``load_dataset``. Must now be forwarded."""
        r = HFDownloader("hf", repo_id="user/dataset", split="validation")
        r.download()
        kwargs = fake_datasets.load_dataset.call_args.kwargs
        assert kwargs.get("split") == "validation"

    def test_download_with_local_path_is_noop(self, fake_datasets, tmp_path):
        local = tmp_path / "mirror"
        local.mkdir()
        r = HFDownloader("hf", repo_id="user/dataset", local_path=local)
        result = r.download()
        assert result is True
        # No network call made.
        fake_datasets.load_dataset.assert_not_called()


class TestHFDownloaderPrepare:
    def test_prepare_includes_new_fields(self):
        r = HFDownloader(
            "hf",
            repo_id="user/dataset",
            name="cfg",
            data_files="train.jsonl.gz",
            split="train",
            streaming=True,
        )
        out = r.prepare()
        assert out == {
            "repo_id": "user/dataset",
            "name": "cfg",
            "data_files": "train.jsonl.gz",
            "split": "train",
            "streaming": True,
            "local_path": None,
        }

    def test_prepare_serializes_local_path(self, tmp_path):
        p = tmp_path / "mirror"
        r = HFDownloader("hf", repo_id="user/dataset", local_path=p)
        out = r.prepare()
        assert out["local_path"] == str(p)

    def test_config_name_stored_separately(self):
        """``HFDownloader.name`` is the varname; the HF config name is
        stored as ``config_name`` to avoid clobbering ``Resource.name``."""
        r = HFDownloader("my_varname", repo_id="user/dataset", name="my_cfg")
        assert r.name == "my_varname"
        assert r.config_name == "my_cfg"


class TestHFDownloaderCheck:
    def test_check_local_path_ok(self, tmp_path):
        local = tmp_path / "mirror"
        local.mkdir()
        r = HFDownloader("hf", repo_id="user/dataset", local_path=local)
        out = r.check()
        # Status is OK and url reflects the local path.
        assert out.status.value == "ok"
        assert out.url == str(local)

    def test_check_local_path_missing(self, tmp_path):
        missing = tmp_path / "does-not-exist"
        r = HFDownloader("hf", repo_id="user/dataset", local_path=missing)
        out = r.check()
        assert out.status.value == "failed"
