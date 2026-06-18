"""Tests for :mod:`datamaestro.helpers` and the resolver integration in
:mod:`datamaestro.download.huggingface`.

Covers:
- ``_EnvHFResolver`` driven by ``DATAMAESTRO_HF_MODELS_CACHE`` /
  ``DATAMAESTRO_HF_DATASETS_CACHE`` (single + colon-separated roots).
- ``get_helpers("hf_resolver")`` always returns the built-in env resolver
  and concatenates plugin-provided resolvers.
- ``HFDownloader.download`` short-circuits via the env resolver.
- ``HFSnapshotDownloader.path`` returns the mirror dir without download.
"""

from __future__ import annotations

import json
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

from datamaestro.helpers import (
    HFResolver,
    _EnvHFResolver,
    get_helpers,
)
from datamaestro.download.huggingface import HFDownloader, HFSnapshotDownloader


# ---- _EnvHFResolver -------------------------------------------------------


class TestEnvHFResolver:
    def test_no_env_returns_none(self, monkeypatch):
        monkeypatch.delenv(_EnvHFResolver.MODELS_ENV, raising=False)
        monkeypatch.delenv(_EnvHFResolver.DATASETS_ENV, raising=False)
        r = _EnvHFResolver()
        assert r.find_model("any/thing", None) is None
        assert r.find_dataset("any/thing", None, None) is None

    def test_finds_model_at_layout(self, monkeypatch, tmp_path):
        root = tmp_path / "models"
        repo = root / "org" / "my-model"
        repo.mkdir(parents=True)
        (repo / "config.json").write_text(json.dumps({"model_type": "x"}))
        monkeypatch.setenv(_EnvHFResolver.MODELS_ENV, str(root))

        r = _EnvHFResolver()
        assert r.find_model("org/my-model", None) == repo

    def test_model_without_config_json_not_found(self, monkeypatch, tmp_path):
        root = tmp_path / "models"
        (root / "org" / "my-model").mkdir(parents=True)  # no config.json
        monkeypatch.setenv(_EnvHFResolver.MODELS_ENV, str(root))
        assert _EnvHFResolver().find_model("org/my-model", None) is None

    def test_finds_dataset(self, monkeypatch, tmp_path):
        root = tmp_path / "datasets"
        repo = root / "org" / "my-ds"
        repo.mkdir(parents=True)
        (repo / "data.parquet").write_bytes(b"")  # any content
        monkeypatch.setenv(_EnvHFResolver.DATASETS_ENV, str(root))

        r = _EnvHFResolver()
        assert r.find_dataset("org/my-ds", None, None) == repo

    def test_multiple_roots_colon_separated(self, monkeypatch, tmp_path):
        root_a = tmp_path / "a"
        root_b = tmp_path / "b"
        for r in (root_a, root_b):
            r.mkdir()
        # Only root_b has the repo.
        repo = root_b / "org" / "elsewhere"
        repo.mkdir(parents=True)
        (repo / "config.json").write_text("{}")
        monkeypatch.setenv(
            _EnvHFResolver.MODELS_ENV,
            f"{root_a}{os.pathsep}{root_b}",
        )
        assert _EnvHFResolver().find_model("org/elsewhere", None) == repo

    def test_protocol_compliance(self):
        # _EnvHFResolver satisfies the HFResolver runtime-checkable Protocol.
        assert isinstance(_EnvHFResolver(), HFResolver)


# ---- get_helpers ----------------------------------------------------------


class TestGetHelpers:
    def test_builtin_env_resolver_always_included(self):
        helpers = get_helpers("hf_resolver")
        assert any(isinstance(h, _EnvHFResolver) for h in helpers)

    def test_unknown_kind_returns_empty(self):
        assert get_helpers("definitely-not-a-helper-kind") == []

    def test_env_resolver_listed_first(self, monkeypatch):
        # Stub a plugin entry-point returning a custom resolver.
        class PluginResolver:
            def find_model(self, repo_id, revision):
                return None

            def find_dataset(self, repo_id, config, data_files):
                return None

        fake_ep = MagicMock()
        fake_ep.load.return_value = lambda: {"hf_resolver": PluginResolver()}

        import datamaestro.helpers as mod

        monkeypatch.setattr(
            "datamaestro.context.iter_entry_points",
            lambda group, name=None: (
                [fake_ep] if group == "datamaestro.helpers" else []
            ),
        )

        helpers = mod.get_helpers("hf_resolver")
        # Built-in env resolver comes first; plugin appended after.
        assert isinstance(helpers[0], _EnvHFResolver)
        assert any(isinstance(h, PluginResolver) for h in helpers[1:])


# ---- HFDownloader resolver integration -----------------------------------


@pytest.fixture
def fake_datasets(monkeypatch):
    fake = types.ModuleType("datasets")
    fake.load_dataset = MagicMock(return_value=MagicMock(name="FakeDataset"))
    monkeypatch.setitem(sys.modules, "datasets", fake)
    return fake


class TestHFDownloaderResolver:
    def test_env_mirror_short_circuits_load_dataset(
        self, monkeypatch, tmp_path, fake_datasets
    ):
        root = tmp_path / "ds"
        repo = root / "org" / "ds"
        repo.mkdir(parents=True)
        monkeypatch.setenv(_EnvHFResolver.DATASETS_ENV, str(root))

        dl = HFDownloader("var", repo_id="org/ds")
        ok = dl.download()

        assert ok is True
        # local_path now set; load_dataset NOT called.
        assert dl.local_path == repo
        fake_datasets.load_dataset.assert_not_called()

    def test_no_mirror_falls_back_to_load_dataset(self, monkeypatch, fake_datasets):
        monkeypatch.delenv(_EnvHFResolver.DATASETS_ENV, raising=False)
        dl = HFDownloader("var", repo_id="org/ds")
        dl.download()
        fake_datasets.load_dataset.assert_called_once()


# ---- HFSnapshotDownloader resolver integration ---------------------------


class TestHFSnapshotDownloaderResolver:
    def test_path_returns_mirror_when_resolver_hits(self, monkeypatch, tmp_path):
        root = tmp_path / "models"
        repo = root / "org" / "model"
        repo.mkdir(parents=True)
        (repo / "config.json").write_text("{}")
        monkeypatch.setenv(_EnvHFResolver.MODELS_ENV, str(root))

        dl = HFSnapshotDownloader("var", repo_id="org/model")
        assert dl.path == repo

    def test_download_skipped_when_resolver_hits(self, monkeypatch, tmp_path):
        root = tmp_path / "models"
        repo = root / "org" / "model"
        repo.mkdir(parents=True)
        (repo / "config.json").write_text("{}")
        monkeypatch.setenv(_EnvHFResolver.MODELS_ENV, str(root))

        # Inject a fake huggingface_hub.snapshot_download that would raise
        # if called — proves it's not invoked when the resolver hits.
        fake = types.ModuleType("huggingface_hub")
        fake.snapshot_download = MagicMock(
            side_effect=AssertionError("snapshot_download should not be called")
        )
        monkeypatch.setitem(sys.modules, "huggingface_hub", fake)

        dl = HFSnapshotDownloader("var", repo_id="org/model")
        # _download is a no-op when resolver serves the repo.
        dl._download(tmp_path / "unused_destination")
        fake.snapshot_download.assert_not_called()
