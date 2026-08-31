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
from experimaestro import Param, Meta, field, Task, PathGenerator

from datamaestro.download.huggingface import hf_download_and_prepare


class HuggingFaceDataset(Base):
    """Adapter for datasets from HuggingFace Hub or local disk mirrors.

    Supports loading datasets via HuggingFace ``datasets`` with support for
    specific configs, data files, splits, streaming mode, or loading directly
    from a local mirror/disk path (e.g. saved via ``Dataset.save_to_disk`` or local folder,
    This can be useful for storing preprocessed versions of the dataset e.g shuffling and filtering).
    """

    repo_id: Param[str]
    """The HuggingFace repository id (e.g. ``user/dataset``)."""

    name: Param[Optional[str]] = field(default=None, ignore_default=True)
    """HuggingFace dataset ``name`` (a.k.a. config)."""

    data_files: Param[Optional[str]] = field(default=None, ignore_default=True)
    """Specific data files to load."""

    split: Param[Optional[str]] = field(default=None, ignore_default=True)
    """Dataset split to load."""

    # TODO: back to Param - did it for keeping hashs
    revision: Meta[Optional[str]] = field(default=None, ignore_default=True)
    """HuggingFace repository git revision (commit SHA, branch, or tag)."""

    streaming: Meta[bool] = field(default=False, ignore_default=True)
    """When True, load the dataset in streaming mode — no local cache."""

    local_path: Meta[Optional[Path]] = field(default=None, ignore_default=True)
    """If set, load from this local mirror instead of the HuggingFace Hub.
    ``Meta`` because the logical dataset is the same regardless of where
    the bytes come from."""

    @property
    def source(self) -> str:
        """Where ``datasets`` should load from: local mirror or Hub repo."""
        return str(self.local_path) if self.local_path is not None else self.repo_id

    def download(self):
        """Materialise the dataset on disk (Arrow shards in the HF cache).

        ``HuggingFaceDataset`` delegates resource management to the
        ``datasets`` library, so the generic download machinery has nothing
        to fetch: without this override, ``prepare_dataset(..., download=True)``
        would be a no-op and the actual download would only happen on the
        first access to :attr:`data` — typically inside a job, or on a
        login/submission node (see issue #27).

        When :attr:`split` is set, only that split is fetched — see
        :func:`~datamaestro.download.huggingface.hf_download_and_prepare`.
        """
        super().download()

        # Streaming mode never materialises anything locally.
        # When local_path is set (e.g. from task output directory or local mirror),
        # there is nothing to download from HF Hub.
        if self.streaming or self.local_path is not None:
            return

        hf_download_and_prepare(
            self.source,
            self.name,
            data_files=self.data_files,
            split=self.split,
            revision=self.revision,
        )

    @cached_property
    def data(self):
        if self.local_path is not None:
            from datasets import load_from_disk

            try:
                return load_from_disk(str(self.local_path))
            except Exception:
                from datasets import load_dataset

                return load_dataset(str(self.local_path))

        if self.streaming:
            try:
                from datasets import load_dataset
            except ModuleNotFoundError:
                logging.error("the datasets library is not installed:")
                logging.error("pip install datasets")
                raise

            return load_dataset(
                self.source,
                self.name,
                data_files=self.data_files,
                split=self.split,
                revision=self.revision,
                streaming=True,
            )

        # Same builder as :meth:`download` — a plain ``load_dataset`` would
        # resolve to a different cache directory when the build was
        # restricted to a single split, and re-fetch the whole dataset. On a
        # warm cache the prepare step is a no-op; on a cold one this is
        # exactly what ``load_dataset`` does internally.
        builder = hf_download_and_prepare(
            self.source,
            self.name,
            data_files=self.data_files,
            split=self.split,
            revision=self.revision,
        )
        return builder.as_dataset(split=self.split)


class FlattenAndShuffleDataset(Task):
    """Concatenate, shuffle, and flatten HuggingFace datasets into a local directory.

    This experimaestro task takes a list of HuggingFace dataset configurations (or any dataset
    objects whose ``.data`` attribute provides a HuggingFace ``Dataset`` or ``IterableDataset``),
    concatenates them, shuffles the combined dataset with a deterministic seed, flattens the
    indices to optimize I/O reading speed, and saves the resulting dataset to disk.

    The task returns a copy of the first dataset configuration with ``local_path`` updated
    to point to the task's output directory and ``streaming`` set to ``False``.

    Attributes:
        samples: List of dataset configurations to process.
        seed: Random seed for shuffling.
        output_dir: Meta parameter for the task's output directory (defaults to "shuffled_dataset").

    Example:
        ```python
        from experimaestro.launcherfinder import find_launcher
        from datamaestro.data.huggingface import HuggingFaceDataset, FlattenAndShuffleDataset

        # Define dataset subsets
        subset1 = HuggingFaceDataset.C(
            repo_id="cross-encoder/ettin-reranker-v1-data", name="agnews", streaming=False
        )
        subset2 = HuggingFaceDataset.C(
            repo_id="cross-encoder/ettin-reranker-v1-data", name="quora", streaming=False
        )

        # Create the preprocessing task
        launcher = find_launcher("cpu")
        shuffled_dataset_config = FlattenAndShuffleDataset.C(
            samples=[subset1, subset2], seed=42
        ).submit(launcher=launcher)

        # Access the resulting dataset config (has local_path set to output_dir)
        print(shuffled_dataset_config.local_path)
        ```
    """

    samples: Param[list[HuggingFaceDataset]]
    """The list of HuggingFace dataset configurations to concatenate and shuffle."""

    seed: Param[int]
    """The random seed used for shuffling."""

    output_dir: Meta[Path] = field(default_factory=PathGenerator("shuffled_dataset"))
    """Output directory for the processed dataset."""

    def execute(self):
        import os
        from datasets import concatenate_datasets

        if not self.samples:
            raise ValueError(
                "FlattenAndShuffleDataset received an empty list of samples."
            )

        logging.info("Concatenating %d HuggingFace datasets...", len(self.samples))
        hf_datasets = [s.data for s in self.samples]

        combined_hf = concatenate_datasets(hf_datasets)
        logging.info("Shuffling dataset with seed=%d...", self.seed)
        shuffled = combined_hf.shuffle(seed=self.seed)

        # Maximize speed by using all allocated CPUs
        num_proc = int(os.environ.get("SLURM_CPUS_PER_TASK", os.cpu_count() or 1))

        # Multiprocessing flatten with larger batches to reduce I/O overhead
        logging.info("Flattening indices with num_proc=%d...", num_proc)
        flattened = shuffled.flatten_indices(num_proc=num_proc, writer_batch_size=10000)

        # Save to the task's output directory concurrently
        logging.info("Saving dataset to %s...", self.output_dir)
        flattened.save_to_disk(self.output_dir, num_proc=num_proc)
        logging.info("Dataset successfully saved to %s.", self.output_dir)

    def __submit__(self, dep, add_action, **kwargs):
        if not self.samples:
            raise ValueError(
                "FlattenAndShuffleDataset received an empty list of samples."
            )

        first = self.samples[0]

        # Generically extract all parameters from the original config
        params = {k: getattr(first, k) for k in first.__xpmtype__.arguments.keys()}

        # Override the path and streaming behavior
        params["local_path"] = self.output_dir
        params["streaming"] = False

        # Returns a new config of the EXACT SAME CLASS
        return dep(first.__class__.C(**params))
