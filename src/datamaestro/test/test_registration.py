"""Tests for `@dataset(variants=...)` registration + query-syntax id routing.

Exercises the integration between `Variants` and the dataset registration
machinery: selector parsing, default filling, variant-keyed caching,
`Base.id` carrying the variant suffix, and backward compatibility with
flat (non-variant) datasets.
"""

from __future__ import annotations

from typing import Optional

import pytest
from experimaestro import Param, Meta, field

from datamaestro.context import Repository
from datamaestro.data import Base
from datamaestro.definitions import Dataset, dataset as dataset_dec
from datamaestro.variants import AxesVariants, Axis, split_id_selector


# ---- Test fixtures --------------------------------------------------------


class _Family(Base):
    """Minimal Base subclass with a couple of Param/Meta fields."""

    name: Param[str]
    streaming: Meta[bool] = field(default=False, ignore_default=True)
    min_score: Param[Optional[float]] = field(default=None, ignore_default=True)


class _FamilyVariants(AxesVariants):
    name = Axis(["a", "b", "c"])
    streaming = Axis([False, True], default=True, type=bool)
    min_score = Axis(type=float, default=None)


def _register_family(
    context,
    *,
    module: str = "datamaestro.config.test_reg.family",
):
    """Build a one-shot @dataset(variants=...) family for testing.

    Returns the wrapper exposed on ``cls.__dataset__``.
    """

    class Family(Dataset):
        def config(self, **kw) -> _Family:
            return _Family.C(**kw)

    # Fake the module so repository_relpath computes a stable id.
    Family.__module__ = module
    Family.__qualname__ = "Family"

    # Apply the decorator with variants.
    wrapped = dataset_dec(url="http://test", variants=_FamilyVariants)(Family)
    _register_in_repo(wrapped, context)
    return wrapped


def _register_flat(context, module="datamaestro.config.test_reg.flat"):
    """Build a plain @dataset (no variants) for backward-compat tests."""

    class Flat(Dataset):
        def config(self) -> _Family:
            return _Family.C(name="fixed")

    Flat.__module__ = module
    Flat.__qualname__ = "Flat"
    wrapped = dataset_dec(url="http://test")(Flat)
    _register_in_repo(wrapped, context)
    return wrapped


class _FixedRepo(Repository):
    """Test repository backed by a fixed in-memory list of datasets.

    Avoids filesystem scanning — we just iterate the wrappers we register.
    """

    NAMESPACE = "test-fixed"
    AUTHOR = "test"
    DESCRIPTION = "Fixed-list repository for registration tests"

    def __init__(self, context):
        super().__init__(context)
        self._fixed = []

    def __iter__(self):
        return iter(self._fixed)

    def add(self, wrapper):
        self._fixed.append(wrapper)


def _register_in_repo(wrapped, context):
    """Attach the wrapper to a fixed test repository so
    ``Repository.search`` can find it."""
    repo = _FixedRepo(context)
    wrapper = wrapped.__dataset__
    wrapper.repository = repo
    repo.add(wrapper)


# ---- split_id_selector ----------------------------------------------------


class TestSplitIdSelector:
    def test_no_selector(self):
        assert split_id_selector("foo.bar") == ("foo.bar", "")

    def test_with_selector(self):
        assert split_id_selector("foo.bar[name=x]") == ("foo.bar", "name=x")

    def test_empty_selector(self):
        assert split_id_selector("foo.bar[]") == ("foo.bar", "")

    def test_whitespace_stripped_from_base(self):
        assert split_id_selector("  foo.bar  ") == ("foo.bar", "")

    def test_selector_content_preserved(self):
        base, sel = split_id_selector("foo[name=x, streaming=true]")
        assert base == "foo"
        assert sel == "name=x, streaming=true"


# ---- dataset decorator + variants accepted ------------------------------


class TestDatasetDecoratorVariants:
    def test_accepts_variants_subclass(self, context):
        wrapped = _register_family(context)
        assert wrapped.__dataset__.variants is not None
        assert isinstance(wrapped.__dataset__.variants, AxesVariants)

    def test_accepts_variants_instance(self, context):
        class Family(Dataset):
            def config(self, **kw) -> _Family:
                return _Family.C(**kw)

        Family.__module__ = "datamaestro.config.test_reg.instance"
        Family.__qualname__ = "Family"

        instance = _FamilyVariants()
        wrapped = dataset_dec(url="http://test", variants=instance)(Family)
        assert wrapped.__dataset__.variants is instance

    def test_rejects_bad_variants_value(self):
        class Family(Dataset):
            def config(self) -> _Family:
                return _Family.C(name="x")

        Family.__module__ = "datamaestro.config.test_reg.bad"
        Family.__qualname__ = "Family"

        with pytest.raises(TypeError):
            dataset_dec(url="http://test", variants="bogus")(Family)

    def test_flat_dataset_has_no_variants(self, context):
        wrapped = _register_flat(context)
        assert wrapped.__dataset__.variants is None


# ---- End-to-end: prepare via variants -----------------------------------


class TestPrepareWithVariants:
    def test_prepare_fills_defaults_and_builds(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        config = wrapper.prepare(variant_kwargs={"name": "a"})
        assert isinstance(config, _Family)
        assert config.name == "a"
        # streaming default applied
        assert config.streaming is True
        # min_score default applied
        assert config.min_score is None

    def test_cache_reuse_for_same_resolved_kwargs(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        c1 = wrapper.prepare(variant_kwargs={"name": "a"})
        # Short-form (defaults elided) vs fully explicit — same resolved kwargs.
        c2 = wrapper.prepare(
            variant_kwargs={
                "name": "a",
                "streaming": True,
                "min_score": None,
            }
        )
        assert c1 is c2

    def test_different_variants_get_different_configs(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        ca = wrapper.prepare(variant_kwargs={"name": "a"})
        cb = wrapper.prepare(variant_kwargs={"name": "b"})
        assert ca is not cb
        assert ca.name == "a"
        assert cb.name == "b"

    def test_variants_with_different_kwargs_cache_independently(self, context):
        """Two variants with different resolved kwargs live in distinct
        cache slots (regardless of whether the kwarg maps to a Meta or
        Param field on the underlying Config)."""
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        c_streaming = wrapper.prepare(variant_kwargs={"name": "a", "streaming": True})
        c_batch = wrapper.prepare(variant_kwargs={"name": "a", "streaming": False})
        assert c_streaming is not c_batch
        assert c_streaming.streaming is True
        assert c_batch.streaming is False

    def test_base_id_carries_variant_suffix(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        config = wrapper.prepare(variant_kwargs={"name": "a"})
        # The id on the Base includes the canonical selector.
        assert config.id.startswith(wrapper.id)
        assert "[" in config.id and "]" in config.id
        assert "name=a" in config.id

    def test_no_kwargs_uses_defaults_when_possible(self, context):
        """With variants, calling _prepare() (no kwargs) should fail
        because `name` has no default."""
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        with pytest.raises(ValueError, match="missing required variant axis"):
            wrapper.prepare()

    def test_invalid_axis_value_rejected(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        with pytest.raises(ValueError, match="not in axis domain"):
            wrapper.prepare(variant_kwargs={"name": "not-in-domain"})


# ---- Repository.search with query-syntax ids ----------------------------


class TestRepositorySearchWithSelector:
    def test_search_ignores_selector_suffix(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__
        repo = wrapper.repository

        # Bare id and selector-form both find the wrapper.
        found_bare = Repository.search(repo, wrapper.id)
        found_sel = Repository.search(repo, f"{wrapper.id}[name=a]")
        assert found_bare is wrapper
        assert found_sel is wrapper

    def test_search_returns_none_for_unknown(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__
        repo = wrapper.repository
        assert Repository.search(repo, "no.such.id[name=x]") is None


# ---- Backward compatibility: flat datasets unchanged --------------------


class TestFlatDatasetsUnchanged:
    def test_flat_dataset_prepare_works(self, context):
        wrapped = _register_flat(context)
        wrapper = wrapped.__dataset__

        config = wrapper.prepare()
        assert isinstance(config, _Family)
        assert config.name == "fixed"

    def test_flat_dataset_cache(self, context):
        """Repeated prepare() on a flat dataset returns the same config."""
        wrapped = _register_flat(context)
        wrapper = wrapped.__dataset__

        c1 = wrapper.prepare()
        c2 = wrapper.prepare()
        assert c1 is c2

    def test_flat_rejects_variant_kwargs(self, context):
        """Passing variant_kwargs to a flat dataset goes down the
        non-variant branch (variant_kwargs is only honoured when variants
        are declared)."""
        wrapped = _register_flat(context)
        wrapper = wrapped.__dataset__

        # Not raised — flat path ignores kwargs when variants=None.
        config = wrapper.prepare(variant_kwargs={"something": "x"})
        assert config.name == "fixed"
