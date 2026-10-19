"""Reproduces issue #25: deferred (scheduler-time) preparation of a
dataset config crashes with

    AttributeError: 'X.XPMConfig' object has no attribute
    '__datamaestro_dataset__'

Since datamaestro 1.15.0, ``Base`` inherits from ``experimaestro.Prepare``,
so a Task that references a dataset config triggers ``config.prepare()`` (a
download) before it runs. But experimaestro *clones* config objects at
task-submit time (``experimaestro.core.objects.config.clone``), copying only
registered ``Param``/``Meta`` values. The link that ``AbstractDataset.prepare``
installs::

    ds.__datamaestro_dataset__ = self

is a raw Python attribute, not a registered parameter, so it is silently
dropped by the clone. When the scheduler later calls ``prepare()`` on the
clone, the attribute is gone and the access raises.

The same crash happens for dynamically generated configs (custom subsets /
unregistered collections) that never had ``__datamaestro_dataset__`` set in
the first place.
"""

from __future__ import annotations

from experimaestro import Param
from experimaestro.core.objects.config import clone

from datamaestro.context import Context
from datamaestro.data import Base
from datamaestro.definitions import Dataset, dataset as dataset_dec


class _Data(Base):
    name: Param[str]


def _make_wrapper(module: str = "datamaestro.config.issue25.flat"):
    """Register a minimal flat dataset and return its wrapper."""

    class Flat(Dataset):
        def config(self) -> _Data:
            return _Data.C(name="fixed")

    Flat.__module__ = module
    Flat.__qualname__ = "Flat"
    return dataset_dec(url="http://test")(Flat).__dataset__


def test_cloned_config_prepare_survives(context, monkeypatch):
    """A config cloned at submit time must still prepare (download) via its
    preserved ``id`` instead of crashing on the dropped raw attribute."""
    wrapper = _make_wrapper()
    config = wrapper.prepare()
    assert "__datamaestro_dataset__" in config.__dict__

    # Simulate experimaestro's submit-time clone (only Params/Metas survive).
    cloned = clone(config)
    assert cloned.id == config.id
    assert "__datamaestro_dataset__" not in cloned.__dict__

    # The scheduler resolves the underlying dataset by id; make it findable
    # and observe the resulting download.
    downloads = []
    monkeypatch.setattr(wrapper, "download", lambda *a, **k: downloads.append(True))
    monkeypatch.setattr(Context, "dataset", lambda self, did: wrapper)

    # Regression: this used to raise AttributeError.
    result = cloned.prepare()
    assert result is cloned
    assert downloads == [True]


def test_dynamic_config_empty_id_prepare_is_noop(context):
    """A dynamically generated config (no registered id, never linked) must
    prepare/download as a graceful no-op instead of crashing."""
    config = _Data.C(name="adhoc")  # never went through AbstractDataset.prepare

    # Regression: both of these used to raise AttributeError.
    assert config.prepare() is config
    config.download()


def test_is_prepared_delegates_to_the_dataset(context, monkeypatch):
    """``Base.is_prepared`` reports what the owning dataset says."""
    wrapper = _make_wrapper("datamaestro.config.issue25.checked")
    config = wrapper.prepare()

    answers = [False, True]
    monkeypatch.setattr(wrapper, "is_prepared", lambda: answers.pop(0))

    assert config.is_prepared() is False
    assert config.is_prepared() is True


def test_is_prepared_survives_the_clone(context, monkeypatch):
    """A config cloned at submit time resolves its dataset back through ``id``.

    Same path as ``prepare()`` (issue #25): the raw
    ``__datamaestro_dataset__`` link does not survive the clone, and
    experimaestro asks a *cloned* config whether it is prepared.
    """
    wrapper = _make_wrapper("datamaestro.config.issue25.checkedclone")
    cloned = clone(wrapper.prepare())
    assert "__datamaestro_dataset__" not in cloned.__dict__

    monkeypatch.setattr(Context, "dataset", lambda self, did: wrapper)
    monkeypatch.setattr(wrapper, "is_prepared", lambda: True)

    assert cloned.is_prepared() is True


def test_dynamic_config_is_prepared(context):
    """Nothing to download means nothing to wait for."""
    config = _Data.C(name="adhoc")  # never went through AbstractDataset.prepare

    assert config.is_prepared() is True
