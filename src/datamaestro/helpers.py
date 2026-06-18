"""Pluggable helpers contributed by third-party packages.

Plugins register one entry point under the group ``datamaestro.helpers``
whose value is a zero-arg callable returning a
``dict[str, Helper | list[Helper]]``. Keys are well-known *kinds*; values
are instances (or lists of instances) of the Protocol for that kind. New
helper kinds can be added later without touching the entry-point
machinery.

Example plugin (``pyproject.toml``)::

    [project.entry-points."datamaestro.helpers"]
    mycluster = "datamaestro_mycluster:datamaestro_helpers"

with::

    # datamaestro_mycluster/__init__.py
    def datamaestro_helpers():
        return {"hf_resolver": MyClusterHFResolver()}

Currently defined helper kinds:

* ``"hf_resolver"`` → :class:`HFResolver` — redirect HF Hub repos to a
  local mirror (e.g. an HPC cluster's shared model/dataset cache).

A built-in :class:`_EnvHFResolver` is always registered for the
``"hf_resolver"`` kind so users can plug in mirrors purely via env vars
(``DATAMAESTRO_HF_MODELS_CACHE``, ``DATAMAESTRO_HF_DATASETS_CACHE`` —
colon-separated directory lists) without writing a plugin.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


# ---- kind: "hf_resolver" --------------------------------------------------


@runtime_checkable
class HFResolver(Protocol):
    """Resolve an HF repo to a local directory.

    Implementations return ``None`` to defer to the next resolver / the
    default network fallback. They MUST be side-effect free and cheap to
    call — the framework may invoke them multiple times.
    """

    def find_model(self, repo_id: str, revision: str | None) -> Path | None: ...

    def find_dataset(
        self,
        repo_id: str,
        config: str | None,
        data_files: str | None,
    ) -> Path | None: ...


# ---- built-in: env-driven HF resolver -------------------------------------


class _EnvHFResolver:
    """Built-in :class:`HFResolver` driven by env-var directory lists.

    Reads two env vars (colon-separated paths, ``PATH`` style):

    * ``DATAMAESTRO_HF_MODELS_CACHE`` — directories searched for HF model
      repos. Layout expected: ``<root>/<repo_id>/`` with a ``config.json``.
    * ``DATAMAESTRO_HF_DATASETS_CACHE`` — directories searched for HF
      datasets. Layout expected: ``<root>/<repo_id>/``.

    Both env vars are re-read on every call, so changes (e.g. via a
    cluster-specific shell rc) take effect without restarting Python.
    """

    MODELS_ENV = "DATAMAESTRO_HF_MODELS_CACHE"
    DATASETS_ENV = "DATAMAESTRO_HF_DATASETS_CACHE"

    @classmethod
    def _roots(cls, env_name: str) -> list[Path]:
        v = os.environ.get(env_name, "")
        if not v:
            return []
        return [Path(p) for p in v.split(os.pathsep) if p]

    def find_model(self, repo_id: str, revision: str | None) -> Path | None:
        for root in self._roots(self.MODELS_ENV):
            p = root / repo_id
            if (p / "config.json").exists():
                logger.info(
                    "[hf_resolver:env] using cached HF model %r from %s",
                    repo_id,
                    p,
                )
                return p
        return None

    def find_dataset(
        self,
        repo_id: str,
        config: str | None,
        data_files: str | None,
    ) -> Path | None:
        for root in self._roots(self.DATASETS_ENV):
            p = root / repo_id
            if p.is_dir():
                logger.info(
                    "[hf_resolver:env] using cached HF dataset %r from %s",
                    repo_id,
                    p,
                )
                return p
        return None


# ---- aggregator -----------------------------------------------------------


def get_helpers(kind: str) -> list:
    """Collect every helper of ``kind`` across all installed plugins.

    A plugin's factory may return either a single helper instance or a
    list of instances under a given key; both forms are accepted and
    flattened into a single result list. For the ``"hf_resolver"`` kind
    the built-in env-driven resolver is always added at the front of the
    returned list, so it gets first chance to serve a request before any
    plugin.
    """
    from datamaestro.context import iter_entry_points

    out: list = []

    # Built-in env resolver: always available, but only fires when its
    # env vars are set + the requested repo lives under one of them.
    if kind == "hf_resolver":
        out.append(_EnvHFResolver())

    for ep in iter_entry_points("datamaestro.helpers"):
        try:
            helpers = ep.load()()
        except Exception:  # noqa: BLE001
            logger.exception("Failed to load helpers from %s", ep)
            continue
        v = helpers.get(kind)
        if v is None:
            continue
        if isinstance(v, list):
            out.extend(v)
        else:
            out.append(v)
    return out
