"""Huggingface datamaestro adapters"""

from functools import cached_property
from typing import Optional
from . import Base
import logging
from experimaestro import Param


class HuggingFaceDataset(Base):
    repo_id: Param[str]
    data_files: Param[Optional[str]] = None
    split: Param[Optional[str]] = None

    @cached_property
    def data(self):
        try:
            from datasets import load_dataset
        except ModuleNotFoundError:
            logging.error("the datasets library is not installed:")
            logging.error("pip install datasets")
            raise

        ds = load_dataset(self.repo_id, data_files=self.data_files, split=self.split)
        return ds
