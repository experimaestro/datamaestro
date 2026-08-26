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
    """Inject a fake ``datasets`` module so tests don't touch the Hub.

    ``load_dataset_builder`` models the real resolution closely enough for
    the split-restriction logic: a builder created without explicit
    ``data_files`` exposes one entry per split (as packaged builders do),
    and one created with explicit ``data_files`` echoes them back.
    """
    fake = types.ModuleType("datasets")
    fake.load_dataset = MagicMock(return_value=MagicMock(name="FakeDataset"))
    fake.load_from_disk = MagicMock(return_value=MagicMock(name="FakeDiskDataset"))

    # Splits the fake Hub repo advertises; tests may override.
    fake.SPLITS = ["train", "validation", "test"]
    # Every builder handed out, in order.
    fake.builders = []

    def load_dataset_builder(source, name=None, data_files=None, revision=None):
        builder = MagicMock(name="FakeBuilder")
        builder.config.data_files = (
            {s: [f"hf://{source}/{s}.parquet"] for s in fake.SPLITS}
            if data_files is None
            else data_files
        )
        fake.builders.append(builder)
        return builder

    fake.load_dataset_builder = MagicMock(side_effect=load_dataset_builder)
    monkeypatch.setitem(sys.modules, "datasets", fake)
    return fake


def prepared(fake_datasets):
    """The builder that was actually prepared (the restricted one, if any)."""
    (builder,) = [b for b in fake_datasets.builders if b.download_and_prepare.called]
    return builder


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
            revision=None,
            streaming=True,
        )

    def test_passes_revision(self, fake_datasets):
        ds = HuggingFaceDataset.C(
            id="test.hf.rev",
            repo_id="user/dataset",
            revision="main_commit_sha",
            streaming=True,
        )
        _ = ds.data
        fake_datasets.load_dataset.assert_called_once_with(
            "user/dataset",
            None,
            data_files=None,
            split=None,
            revision="main_commit_sha",
            streaming=True,
        )

    def test_local_path_loads_from_disk(self, fake_datasets, tmp_path):
        local = tmp_path / "mirror"
        local.mkdir()
        ds = HuggingFaceDataset.C(
            id="test.hf.2",
            repo_id="user/dataset",
            local_path=local,
        )
        data = ds.data
        fake_datasets.load_from_disk.assert_called_once_with(str(local))
        assert data is fake_datasets.load_from_disk.return_value

    def test_local_path_fallback_to_load_dataset(self, fake_datasets, tmp_path):
        local = tmp_path / "mirror"
        local.mkdir()
        fake_datasets.load_from_disk.side_effect = Exception("Not a disk dataset")
        ds = HuggingFaceDataset.C(
            id="test.hf.2b",
            repo_id="user/dataset",
            local_path=local,
        )
        data = ds.data
        fake_datasets.load_dataset.assert_called_once_with(str(local))
        assert data is fake_datasets.load_dataset.return_value

    def test_default_args(self, fake_datasets):
        """Non-streaming access goes through the builder, and returns what
        ``as_dataset`` gives us (not ``load_dataset``)."""
        ds = HuggingFaceDataset.C(id="test.hf.3", repo_id="user/dataset")
        data = ds.data
        fake_datasets.load_dataset_builder.assert_called_once_with(
            "user/dataset",
            None,  # name
            data_files=None,
            revision=None,
        )
        builder = prepared(fake_datasets)
        builder.as_dataset.assert_called_once_with(split=None)
        assert data is builder.as_dataset.return_value
        fake_datasets.load_dataset.assert_not_called()

    def test_data_reuses_the_download_builder(self, fake_datasets):
        """Reading a single split must go through the same (restricted)
        builder as ``download`` — a plain ``load_dataset`` would resolve to
        another cache directory and re-fetch everything."""
        ds = HuggingFaceDataset.C(id="test.hf.4", repo_id="user/dataset", split="train")
        data = ds.data
        builder = prepared(fake_datasets)
        assert builder.config.data_files == {
            "train": ["hf://user/dataset/train.parquet"]
        }
        builder.as_dataset.assert_called_once_with(split="train")
        assert data is builder.as_dataset.return_value
        fake_datasets.load_dataset.assert_not_called()


# ---- HuggingFaceDataset.download (issue #27) -----------------------------


class TestHuggingFaceDatasetDownload:
    def test_download_prepares_builder(self, fake_datasets):
        """``download()`` must materialise the Arrow shards, and must do so
        without instantiating the ``Dataset`` object in memory."""
        ds = HuggingFaceDataset.C(
            id="test.hf.dl.1",
            repo_id="user/dataset",
            name="config-a",
            data_files="train.jsonl.gz",
        )
        ds.download()

        fake_datasets.load_dataset_builder.assert_called_once_with(
            "user/dataset",
            "config-a",
            data_files="train.jsonl.gz",
            revision=None,
        )
        prepared(fake_datasets).download_and_prepare.assert_called_once_with(
            verification_mode="no_checks"
        )
        # No in-RAM instantiation of the dataset.
        fake_datasets.load_dataset.assert_not_called()

    def test_download_with_local_path_is_noop(self, fake_datasets, tmp_path):
        local = tmp_path / "mirror"
        local.mkdir()
        ds = HuggingFaceDataset.C(
            id="test.hf.dl.2",
            repo_id="user/dataset",
            local_path=local,
        )
        ds.download()
        fake_datasets.load_dataset_builder.assert_not_called()

    def test_download_streaming_is_noop(self, fake_datasets):
        ds = HuggingFaceDataset.C(
            id="test.hf.dl.3", repo_id="user/dataset", streaming=True
        )
        ds.download()
        fake_datasets.load_dataset_builder.assert_not_called()

    def test_prepare_triggers_download(self, fake_datasets):
        """``prepare()`` is what experimaestro calls before a task runs."""
        ds = HuggingFaceDataset.C(id="test.hf.dl.4", repo_id="user/dataset")
        assert ds.prepare() is ds
        prepared(fake_datasets).download_and_prepare.assert_called_once()


class TestHuggingFaceDatasetSplitRestriction:
    """Only the requested split should be fetched (``download_and_prepare``
    has no split argument, so we restrict the builder's data files)."""

    def _download(self, fake_datasets, **kwargs):
        ds = HuggingFaceDataset.C(id="test.hf.sp", repo_id="user/dataset", **kwargs)
        ds.download()
        return prepared(fake_datasets)

    def test_restricts_to_requested_split(self, fake_datasets):
        builder = self._download(fake_datasets, split="validation")
        assert builder.config.data_files == {
            "validation": ["hf://user/dataset/validation.parquet"]
        }
        # Split checks compare against the metadata, which lists all splits.
        builder.download_and_prepare.assert_called_once_with(
            verification_mode="no_checks"
        )

    def test_restricts_on_a_sliced_split(self, fake_datasets):
        builder = self._download(fake_datasets, split="train[:10%]")
        assert list(builder.config.data_files) == ["train"]

    def test_no_restriction_for_compound_split(self, fake_datasets):
        """``train+test`` spans several splits: prepare everything with verification disabled."""
        builder = self._download(fake_datasets, split="train+test")
        assert list(builder.config.data_files) == ["train", "validation", "test"]
        builder.download_and_prepare.assert_called_once_with(
            verification_mode="no_checks"
        )

    def test_no_restriction_without_split(self, fake_datasets):
        builder = self._download(fake_datasets)
        assert list(builder.config.data_files) == ["train", "validation", "test"]
        builder.download_and_prepare.assert_called_once_with()

    def test_no_restriction_for_unknown_split(self, fake_datasets):
        """A split the builder does not advertise: leave it alone and let
        ``datasets`` raise its own error downstream."""
        builder = self._download(fake_datasets, split="nonexistent")
        assert list(builder.config.data_files) == ["train", "validation", "test"]

    def test_no_restriction_when_already_single_split(self, fake_datasets):
        fake_datasets.SPLITS = ["train"]
        builder = self._download(fake_datasets, split="train")
        # Split is set: verification is bypassed.
        assert fake_datasets.load_dataset_builder.call_count == 1
        builder.download_and_prepare.assert_called_once_with(
            verification_mode="no_checks"
        )

    def test_no_restriction_for_script_builder(self, fake_datasets):
        """A script-based builder exposes no per-split ``data_files``."""

        def no_data_files(source, name=None, data_files=None, revision=None):
            builder = MagicMock(name="ScriptBuilder")
            builder.config.data_files = None
            fake_datasets.builders.append(builder)
            return builder

        fake_datasets.load_dataset_builder.side_effect = no_data_files
        builder = self._download(fake_datasets, split="train")
        assert fake_datasets.load_dataset_builder.call_count == 1
        builder.download_and_prepare.assert_called_once_with(
            verification_mode="no_checks"
        )

    def test_data_files_bypasses_split_verification(self, fake_datasets):
        ds = HuggingFaceDataset.C(
            id="test.hf.df",
            repo_id="user/dataset",
            data_files="data.parquet",
        )
        ds.download()
        prepared(fake_datasets).download_and_prepare.assert_called_once_with(
            verification_mode="no_checks"
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

    def test_different_revision_different_identity(self):
        a = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", revision="rev1")
        b = HuggingFaceDataset.C(id="test.id", repo_id="user/dataset", revision="rev2")
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
        )
        result = r.download()
        assert result is True
        fake_datasets.load_dataset_builder.assert_called_once_with(
            "user/dataset",
            "cfg",
            data_files="train.jsonl.gz",
            revision=None,
        )
        # Downloading must not instantiate the dataset in memory.
        fake_datasets.load_dataset.assert_not_called()

    def test_download_passes_split_regression(self, fake_datasets):
        """Regression: pre-existing bug where ``split`` was accepted but
        dropped. It must still drive what gets fetched."""
        r = HFDownloader("hf", repo_id="user/dataset", split="validation")
        r.download()
        assert list(prepared(fake_datasets).config.data_files) == ["validation"]

    def test_download_streaming_is_noop(self, fake_datasets):
        """Streaming materialises nothing: no builder, no dataset."""
        r = HFDownloader("hf", repo_id="user/dataset", streaming=True)
        assert r.download() is True
        fake_datasets.load_dataset_builder.assert_not_called()
        fake_datasets.load_dataset.assert_not_called()

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
            "revision": None,
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


class TestHuggingFaceOfflineHashedFallback:
    def test_offline_hashed_fallback(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock
        import sys
        import types
        from datamaestro.download.huggingface import hf_builder

        cache_dir = tmp_path / "hf_cache"
        repo_dir = cache_dir / "user___dataset"
        hashed_dir = repo_dir / "quora-ccbd7fec3e15cba7"
        hashed_dir.mkdir(parents=True)

        fake = types.ModuleType("datasets")
        fake.config = types.SimpleNamespace(HF_DATASETS_CACHE=str(cache_dir))

        def mock_load_builder(source, name=None, data_files=None):
            if name == "quora" and data_files is None:
                raise ValueError(
                    "Couldn't find cache for user/dataset for config 'quora'"
                )
            builder = MagicMock(name=f"Builder_{name}")
            builder.config.name = name
            return builder

        fake.load_dataset_builder = mock_load_builder
        monkeypatch.setitem(sys.modules, "datasets", fake)
        monkeypatch.setitem(sys.modules, "datasets.config", fake.config)

        builder, restricted = hf_builder("user/dataset", name="quora", split="train")
        assert restricted is True
        assert builder.config.name == "quora-ccbd7fec3e15cba7"
