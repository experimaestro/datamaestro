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

from datamaestro.context import Repository, _resolve_dataset_id
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


# ---- Dataset ID computation through variants ----------------------------


def _register_with_variants(context, variants_cls, module):
    """Register a one-shot family with a provided variants class."""

    class Family(Dataset):
        def config(self, **kw) -> _Family:
            return _Family.C(**kw)

    Family.__module__ = module
    Family.__qualname__ = "Family"
    wrapped = dataset_dec(url="http://test", variants=variants_cls)(Family)
    _register_in_repo(wrapped, context)
    return wrapped


class TestBaseIDFromVariants:
    """End-to-end: the ``Base.id`` on a prepared config is
    ``wrapper.id + format_selector(resolved)``, with the elision +
    determinism rules applied."""

    @staticmethod
    def _bare_id(config, wrapper):
        """Strip the ``@<repo>`` suffix that ``setDataIDs`` appends so
        tests can assert against the variant-id portion directly."""
        return config.id.rsplit("@", 1)[0]

    def test_id_includes_full_selector_without_elide(self, context):
        wrapped = _register_family(context)  # declared-axes family
        wrapper = wrapped.__dataset__
        config = wrapper.prepare(variant_kwargs={"name": "a"})
        bare = self._bare_id(config, wrapper)
        # Every axis is non-elidable → suffix includes defaults too.
        assert bare.startswith(f"{wrapper.id}[")
        assert bare.endswith("]")
        body = bare[len(wrapper.id) + 1 : -1]
        keys = [fragment.split("=", 1)[0] for fragment in body.split(",")]
        # Keys are sorted alphabetically.
        assert keys == sorted(keys)
        # All three axes are represented.
        assert set(keys) == {"name", "streaming", "min_score"}

    def test_fully_defaulted_elidable_family_drops_brackets(self, context):
        class AllElidable(AxesVariants):
            name = Axis(["fixed"], default="fixed", elide_default=True)
            streaming = Axis([False, True], default=True, type=bool, elide_default=True)
            min_score = Axis(type=float, default=None, elide_default=True)

        wrapped = _register_with_variants(
            context, AllElidable, "datamaestro.config.test_reg.all_elide"
        )
        wrapper = wrapped.__dataset__
        config = wrapper.prepare(variant_kwargs={})
        # No axis takes a non-default → the suffix collapses away.
        assert self._bare_id(config, wrapper) == wrapper.id

    def test_elidable_axis_appears_when_non_default(self, context):
        class MixedElidable(AxesVariants):
            name = Axis(["a", "b"])  # required, always shown
            streaming = Axis([False, True], default=True, type=bool, elide_default=True)

        wrapped = _register_with_variants(
            context, MixedElidable, "datamaestro.config.test_reg.mixed_elide"
        )
        wrapper = wrapped.__dataset__

        default = wrapper.prepare(variant_kwargs={"name": "a"})
        assert self._bare_id(default, wrapper) == f"{wrapper.id}[name=a]"

        toggled = wrapper.prepare(variant_kwargs={"name": "a", "streaming": False})
        assert (
            self._bare_id(toggled, wrapper) == f"{wrapper.id}[name=a,streaming=false]"
        )

    def test_id_stable_when_new_elided_axis_added(self, context):
        """The core back-compat guarantee: id(old_variants_declaration,
        old_kwargs) == id(new_variants_declaration_with_extra_elided_axis,
        old_kwargs)."""

        class VOld(AxesVariants):
            name = Axis(["a", "b"])

        class VNew(AxesVariants):
            name = Axis(["a", "b"])
            cache_mode = Axis(["mem", "disk"], default="mem", elide_default=True)

        wrapped_old = _register_with_variants(
            context, VOld, "datamaestro.config.test_reg.v_old"
        )
        wrapped_new = _register_with_variants(
            context, VNew, "datamaestro.config.test_reg.v_new"
        )
        # Both families share the same relative id path (different
        # modules → different wrapper.id, so compare the variant suffix
        # directly).
        suffix_old = wrapped_old.__dataset__.variants.format_selector({"name": "a"})
        suffix_new = wrapped_new.__dataset__.variants.format_selector({"name": "a"})
        assert suffix_old == suffix_new == "[name=a]"

    def test_mixed_elide_and_in_id_drops_brackets(self, context):
        """Mixed ``elide_default`` + ``in_id=False``: when every axis
        is stripped, the ``Base.id`` ends at the wrapper id — no
        ``[]`` suffix, no half-formed bracket pair."""

        class Collapsible(AxesVariants):
            name = Axis(["fixed"], default="fixed", elide_default=True)
            download_mode = Axis(["fast", "slow"], default="fast", in_id=False)

        class Family(Dataset):
            def config(self, **kw) -> _Family:
                kw.pop("download_mode", None)
                return _Family.C(**kw)

        Family.__module__ = "datamaestro.config.test_reg.collapsible"
        Family.__qualname__ = "Family"
        wrapped = dataset_dec(url="http://test", variants=Collapsible)(Family)
        _register_in_repo(wrapped, context)
        wrapper = wrapped.__dataset__

        # All defaults, in_id=False axis absent → bare id.
        bare = self._bare_id(wrapper.prepare(variant_kwargs={}), wrapper)
        assert bare == wrapper.id
        assert "[" not in bare and "]" not in bare

        # in_id=False axis on non-default → still no brackets.
        bare = self._bare_id(
            wrapper.prepare(variant_kwargs={"download_mode": "slow"}),
            wrapper,
        )
        assert bare == wrapper.id
        assert "[" not in bare and "]" not in bare

    def test_in_id_false_axis_never_affects_id(self, context):
        """End-to-end: an axis marked ``in_id=False`` is absent from
        ``Base.id`` regardless of the chosen value. The family's
        ``config()`` is responsible for consuming (or dropping) the
        kwarg — ``in_id=False`` only controls the *id*."""

        class DownloadFlag(AxesVariants):
            name = Axis(["a"], default="a")
            download_mode = Axis(["fast", "slow"], default="fast", in_id=False)

        class Family(Dataset):
            def config(self, **kw) -> _Family:
                # `download_mode` is an `in_id=False` download-time flag
                # — it doesn't feed into the built config.
                kw.pop("download_mode", None)
                return _Family.C(**kw)

        Family.__module__ = "datamaestro.config.test_reg.dl_flag"
        Family.__qualname__ = "Family"
        wrapped = dataset_dec(url="http://test", variants=DownloadFlag)(Family)
        _register_in_repo(wrapped, context)
        wrapper = wrapped.__dataset__

        fast_id = self._bare_id(
            wrapper.prepare(variant_kwargs={"download_mode": "fast"}),
            wrapper,
        )
        slow_id = self._bare_id(
            wrapper.prepare(variant_kwargs={"download_mode": "slow"}),
            wrapper,
        )
        # Same id despite different download_mode — and the axis name
        # does not appear in the suffix at all.
        assert fast_id == slow_id
        assert "download_mode" not in fast_id

    def test_id_invariant_under_kwargs_order(self, context):
        """Deterministic ordering promise: permuting kwargs doesn't
        change the resulting id."""

        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        c1 = wrapper.prepare(
            variant_kwargs={"name": "a", "streaming": True, "min_score": None}
        )
        c2 = wrapper.prepare(
            variant_kwargs={"min_score": None, "streaming": True, "name": "a"}
        )
        # Same resolved kwargs → cached to the same config + same id.
        assert c1 is c2
        assert c1.id == c2.id


# ---- prepare_dataset / get_dataset variant= kwarg ----------------------


class TestVariantKwargRouting:
    """`_resolve_dataset_id` accepts either a selector in the id or a
    ``variant={}`` kwarg. Exercising it directly lets us skip the
    entry-point repository lookup while still covering the public API's
    resolution logic (both `prepare_dataset` and `get_dataset` share it).
    """

    def test_variant_kwarg_resolves_like_selector(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        _, via_kwarg = _resolve_dataset_id(wrapper, variant={"name": "a"})
        # For comparison, resolve what the selector form would produce.
        expected = wrapper.variants.resolve(name="a")
        assert via_kwarg == expected

    def test_variant_kwarg_fills_defaults(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        _, kwargs = _resolve_dataset_id(wrapper, variant={"name": "b"})
        # streaming and min_score defaults come from the axes.
        assert kwargs == {"name": "b", "streaming": True, "min_score": None}

    def test_prepare_dataset_with_variant_kwarg_builds(self, context):
        from datamaestro.context import prepare_dataset

        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        config = prepare_dataset(wrapper, variant={"name": "a"})
        assert config.name == "a"
        assert config.streaming is True

    def test_prepare_dataset_kwarg_matches_selector_form(self, context):
        """Both forms route to the same cached config."""
        from datamaestro.context import prepare_dataset

        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        via_kwarg = prepare_dataset(wrapper, variant={"name": "a"})
        via_direct = wrapper.prepare(variant_kwargs={"name": "a"})
        assert via_kwarg is via_direct

    def test_both_selector_and_variant_kwarg_rejected(self, context, monkeypatch):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        # The `_FixedRepo` isn't entry-point-registered, so stub the
        # find path to return our wrapper and exercise the selector+
        # variant conflict check.
        from datamaestro.definitions import AbstractDataset

        monkeypatch.setattr(
            AbstractDataset,
            "find",
            staticmethod(lambda name, context=None: wrapper),
        )

        with pytest.raises(ValueError, match="both a selector"):
            _resolve_dataset_id(f"{wrapper.id}[name=a]", variant={"name": "b"})

    def test_variant_kwarg_on_flat_dataset_rejected(self, context):
        wrapped = _register_flat(context)
        wrapper = wrapped.__dataset__

        with pytest.raises(ValueError, match="does not declare variants"):
            _resolve_dataset_id(wrapper, variant={"something": "x"})

    def test_variant_kwarg_unknown_axis_rejected(self, context):
        wrapped = _register_family(context)
        wrapper = wrapped.__dataset__

        with pytest.raises(ValueError, match="unknown variant axes"):
            _resolve_dataset_id(wrapper, variant={"bogus": "x"})


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
