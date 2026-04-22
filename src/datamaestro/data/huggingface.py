"""Huggingface datamaestro adapters.

Convention: ``Param`` vs ``Meta``
    - ``Param[T]`` contributes to the dataset's experimaestro identity hash
      — use for fields that change *which* dataset is loaded
      (``repo_id``, ``name``, ``data_files``, ``split``).
    - ``Meta[T]`` is ignored by the identity hash — use for fields that
      only change *how* the dataset is loaded (``streaming``,
      ``local_path``). Two objects that only differ on ``Meta`` fields
      describe the same logical dataset.
"""

from functools import cached_property
from pathlib import Path
from typing import Optional
from . import Base
import logging
from experimaestro import Param, Meta


class HuggingFaceDataset(Base):
    repo_id: Param[str]
    """The HuggingFace repository id (e.g. ``user/dataset``)."""

    name: Param[Optional[str]] = None
    """HuggingFace dataset ``name`` (a.k.a. config)."""

    data_files: Param[Optional[str]] = None
    """Specific data files to load."""

    split: Param[Optional[str]] = None
    """Dataset split to load."""

    streaming: Meta[bool] = False
    """When True, load the dataset in streaming mode — no local cache."""

    local_path: Meta[Optional[Path]] = None
    """If set, load from this local mirror instead of the HuggingFace Hub.
    ``Meta`` because the logical dataset is the same regardless of where
    the bytes come from."""

    @cached_property
    def data(self):
        try:
            from datasets import load_dataset
        except ModuleNotFoundError:
            logging.error("the datasets library is not installed:")
            logging.error("pip install datasets")
            raise

        source = str(self.local_path) if self.local_path is not None else self.repo_id
        ds = load_dataset(
            source,
            self.name,
            data_files=self.data_files,
            split=self.split,
            streaming=self.streaming,
        )
        return ds
