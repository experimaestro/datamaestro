"""Tests for the variants facility (datamaestro.variants).

Covers the standalone module: Axis coercion and validation, AxesVariants
declaration/resolve/parse/format/enumerate, and the Variants abstract
contract.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Optional

import pytest

from datamaestro.variants import (
    MISSING,
    Axis,
    AxesVariants,
    Variants,
)


# ---- Axis ----------------------------------------------------------------


class TestAxis:
    def test_enumerable_and_has_default(self):
        a = Axis(["x", "y"], default="x")
        assert a.enumerable is True
        assert a.has_default is True

    def test_open_axis(self):
        a = Axis(type=float)
        assert a.enumerable is False
        assert a.has_default is False

    def test_missing_sentinel(self):
        a = Axis(type=str)
        assert a.default is MISSING
        assert a.has_default is False

    def test_infer_type_from_homogeneous_domain(self):
        assert Axis(["a", "b"]).type is str
        assert Axis([1, 2, 3]).type is int
        # Heterogeneous → no inference
        assert Axis([1, "x"]).type is None

    def test_domain_must_be_list(self):
        with pytest.raises(TypeError):
            Axis(("not", "a", "list"))  # type: ignore[arg-type]

    def test_coerce_str(self):
        a = Axis(type=str)
        assert a.coerce("hello") == "hello"

    def test_coerce_int(self):
        a = Axis(type=int)
        assert a.coerce("42") == 42
        assert a.coerce(42) == 42

    def test_coerce_float(self):
        a = Axis(type=float)
        assert a.coerce("3.14") == 3.14
        assert a.coerce("3") == 3.0

    def test_coerce_bool_true_variants(self):
        a = Axis(type=bool)
        for raw in ("true", "True", "TRUE", "yes", "1"):
            assert a.coerce(raw) is True

    def test_coerce_bool_false_variants(self):
        a = Axis(type=bool)
        for raw in ("false", "False", "FALSE", "no", "0"):
            assert a.coerce(raw) is False

    def test_coerce_bool_invalid(self):
        a = Axis(type=bool)
        with pytest.raises(ValueError):
            a.coerce("maybe")

    def test_coerce_none_markers(self):
        a = Axis(type=float)
        assert a.coerce("null") is None
        assert a.coerce("None") is None
        assert a.coerce("none") is None
        assert a.coerce(None) is None

    def test_coerce_optional_type(self):
        a = Axis(type=Optional[float])
        assert a.coerce("3.14") == 3.14
        assert a.coerce("null") is None

    def test_coerce_open_axis_without_type_keeps_string(self):
        a = Axis()
        assert a.coerce("raw") == "raw"

    def test_validate_enumerable_accepts_domain_value(self):
        a = Axis(["x", "y"])
        a.validate("x")  # no raise

    def test_validate_enumerable_rejects_out_of_domain(self):
        a = Axis(["x", "y"])
        with pytest.raises(ValueError):
            a.validate("z")

    def test_validate_open_axis_accepts_anything(self):
        a = Axis(type=float)
        a.validate(3.14)
        a.validate("anything")  # open axes don't validate content

    def test_elide_default_requires_default(self):
        with pytest.raises(ValueError, match="elide_default"):
            Axis(["a", "b"], elide_default=True)

    def test_elide_default_stored(self):
        a = Axis(["a", "b"], default="a", elide_default=True)
        assert a.elide_default is True

    def test_elide_default_off_by_default(self):
        a = Axis(["a", "b"], default="a")
        assert a.elide_default is False

    def test_in_id_on_by_default(self):
        a = Axis(["a", "b"], default="a")
        assert a.in_id is True

    def test_in_id_stored(self):
        a = Axis(["fast", "slow"], default="fast", in_id=False)
        assert a.in_id is False


# ---- AxesVariants declarative + imperative --------------------------------


class MyVariants(AxesVariants):
    name = Axis(["agnews", "reddit"])
    streaming = Axis([False, True], default=True, type=bool)
    threshold = Axis(type=float, default=None)


class TestAxesVariantsDeclaration:
    def test_subclass_collects_axes(self):
        v = MyVariants()
        assert set(v.axes) == {"name", "streaming", "threshold"}

    def test_subclass_axes_are_the_declared_objects(self):
        v = MyVariants()
        assert v.axes["name"] is MyVariants.name
        assert v.axes["streaming"] is MyVariants.streaming

    def test_imperative_construction(self):
        v = AxesVariants(
            name=Axis(["agnews", "reddit"]),
            streaming=Axis([False, True], default=True, type=bool),
        )
        assert set(v.axes) == {"name", "streaming"}

    def test_imperative_merges_with_subclass_axes(self):
        """Passing new axes via __init__ on a subclass merges them in."""
        v = MyVariants(extra=Axis(["a", "b"], default="a"))
        assert set(v.axes) == {"name", "streaming", "threshold", "extra"}

    def test_imperative_override(self):
        """Imperative axis takes precedence over declared axis with same name."""
        override = Axis(["one", "two"])
        v = MyVariants(name=override)
        assert v.axes["name"] is override

    def test_imperative_rejects_non_axis(self):
        with pytest.raises(TypeError):
            AxesVariants(bogus="not an axis")  # type: ignore[arg-type]

    def test_inherited_axes(self):
        class Parent(AxesVariants):
            a = Axis(["x"], default="x")

        class Child(Parent):
            b = Axis(["y"], default="y")

        v = Child()
        assert set(v.axes) == {"a", "b"}

    def test_axes_property_is_copy(self):
        v = MyVariants()
        snapshot = v.axes
        snapshot["name"] = Axis(["bogus"])
        # Original is unchanged
        assert v.axes["name"] is MyVariants.name


# ---- resolve --------------------------------------------------------------


class TestResolve:
    def test_fills_defaults(self):
        v = MyVariants()
        result = v.resolve(name="agnews")
        assert result == {
            "name": "agnews",
            "streaming": True,
            "threshold": None,
        }

    def test_returns_every_axis(self):
        v = MyVariants()
        result = v.resolve(name="agnews")
        assert set(result) == set(v.axes)

    def test_raises_on_missing_required(self):
        v = MyVariants()
        with pytest.raises(ValueError, match="missing required variant axis"):
            v.resolve()

    def test_raises_on_unknown_axis(self):
        v = MyVariants()
        with pytest.raises(ValueError, match="unknown variant axes"):
            v.resolve(name="agnews", bogus="x")

    def test_validates_domain(self):
        v = MyVariants()
        with pytest.raises(ValueError, match="not in axis domain"):
            v.resolve(name="not_a_config")

    def test_accepts_all_defaults_when_required_provided(self):
        v = MyVariants()
        result = v.resolve(name="reddit", streaming=False, threshold=3.0)
        assert result == {"name": "reddit", "streaming": False, "threshold": 3.0}


# ---- parse_selector -------------------------------------------------------


class TestParseSelector:
    def test_empty_selector_returns_empty_dict(self):
        v = MyVariants()
        assert v.parse_selector("") == {}
        assert v.parse_selector("[]") == {}
        assert v.parse_selector("[  ]") == {}

    def test_single_kv(self):
        v = MyVariants()
        assert v.parse_selector("[name=agnews]") == {"name": "agnews"}

    def test_multiple_kvs(self):
        v = MyVariants()
        out = v.parse_selector("[name=agnews,streaming=false]")
        assert out == {"name": "agnews", "streaming": False}

    def test_whitespace_tolerated(self):
        v = MyVariants()
        out = v.parse_selector("[ name = agnews , streaming = true ]")
        assert out == {"name": "agnews", "streaming": True}

    def test_brackets_optional(self):
        v = MyVariants()
        assert v.parse_selector("name=agnews") == {"name": "agnews"}

    def test_null_coerces(self):
        v = MyVariants()
        assert v.parse_selector("[threshold=null]") == {"threshold": None}

    def test_float_coerces(self):
        v = MyVariants()
        assert v.parse_selector("[threshold=3.0]") == {"threshold": 3.0}

    def test_unknown_key_raises(self):
        v = MyVariants()
        with pytest.raises(ValueError, match="unknown variant axis"):
            v.parse_selector("[bogus=x]")

    def test_malformed_fragment_raises(self):
        v = MyVariants()
        with pytest.raises(ValueError, match="malformed selector fragment"):
            v.parse_selector("[nonsense]")


# ---- format_selector ------------------------------------------------------


class TestFormatSelector:
    def test_includes_every_axis(self):
        v = MyVariants()
        out = v.format_selector({"name": "agnews"})
        # All three axes present
        assert "name=agnews" in out
        assert "streaming=true" in out
        assert "threshold=null" in out

    def test_sorted_keys(self):
        v = MyVariants()
        out = v.format_selector({"name": "agnews"})
        # Sorted alphabetically
        assert out == "[name=agnews,streaming=true,threshold=null]"

    def test_bool_lowercase(self):
        v = MyVariants()
        out = v.format_selector({"name": "reddit", "streaming": False})
        assert "streaming=false" in out

    def test_none_as_null(self):
        v = MyVariants()
        out = v.format_selector({"name": "reddit", "threshold": None})
        assert "threshold=null" in out

    def test_round_trip_with_parse(self):
        v = MyVariants()
        formatted = v.format_selector({"name": "agnews", "threshold": 3.0})
        parsed = v.parse_selector(formatted)
        # Canonical round-trip: parse(format(resolve(x))) == resolve(x)
        assert parsed == v.resolve(name="agnews", threshold=3.0)

    def test_two_equivalent_selectors_format_identically(self):
        v = MyVariants()
        # Short form (defaults elided)
        a = v.format_selector({"name": "agnews"})
        # Full form (defaults explicit)
        b = v.format_selector({"name": "agnews", "streaming": True, "threshold": None})
        assert a == b


# ---- format_selector: elision + ID-computation guarantees -----------------


class TestFormatSelectorElision:
    """``elide_default=True`` drops the axis from the formatted suffix
    whenever the resolved value equals the declared default. When every
    axis elides, the whole ``[...]`` part disappears — which is how a
    family can grow new axes without disturbing existing ids.
    """

    def test_default_axis_without_elide_still_emitted(self):
        class V(AxesVariants):
            name = Axis(["a"], default="a")

        v = V()
        assert v.format_selector({}) == "[name=a]"

    def test_elide_default_omits_axis_at_default(self):
        class V(AxesVariants):
            name = Axis(["a"], default="a", elide_default=True)

        v = V()
        assert v.format_selector({}) == ""
        assert v.format_selector({"name": "a"}) == ""

    def test_elide_default_keeps_axis_when_non_default(self):
        class V(AxesVariants):
            name = Axis(["a", "b"], default="a", elide_default=True)

        v = V()
        assert v.format_selector({"name": "b"}) == "[name=b]"

    def test_mixed_elision_strips_only_defaulted_ones(self):
        class V(AxesVariants):
            name = Axis(["a", "b"])  # required, no default → always shown
            streaming = Axis([False, True], default=True, type=bool, elide_default=True)
            threshold = Axis(type=float, default=None, elide_default=True)

        v = V()
        # Everything elidable at its default: only `name` survives.
        assert v.format_selector({"name": "a"}) == "[name=a]"
        # One elidable axis at a non-default value reappears.
        assert (
            v.format_selector({"name": "a", "streaming": False})
            == "[name=a,streaming=false]"
        )
        # threshold=0.5 (non-default) also reappears.
        assert (
            v.format_selector({"name": "a", "threshold": 0.5})
            == "[name=a,threshold=0.5]"
        )

    def test_none_default_is_elidable(self):
        """Covers the common ``default=None`` open-axis case."""

        class V(AxesVariants):
            threshold = Axis(type=float, default=None, elide_default=True)

        v = V()
        assert v.format_selector({}) == ""
        assert v.format_selector({"threshold": 0.0}) == "[threshold=0.0]"


class TestFormatSelectorInId:
    """``in_id=False`` strips the axis from the selector *regardless* of
    value — so it never appears in the dataset id, even when the user
    picked a non-default."""

    def test_in_id_false_omits_axis_even_when_non_default(self):
        class V(AxesVariants):
            name = Axis(["a", "b"])
            download_mode = Axis(["fast", "slow"], default="fast", in_id=False)

        v = V()
        # At default: axis is gone.
        assert v.format_selector({"name": "a"}) == "[name=a]"
        # At non-default: axis is *still* gone.
        assert v.format_selector({"name": "a", "download_mode": "slow"}) == "[name=a]"

    def test_in_id_false_works_without_default(self):
        """Unlike ``elide_default``, ``in_id=False`` has no
        default requirement — it unconditionally hides the axis."""

        class V(AxesVariants):
            name = Axis(["a", "b"])
            download_mode = Axis(["fast", "slow"], in_id=False)

        v = V()
        # Required + in_id=False: must provide, but never appears in id.
        assert v.format_selector({"name": "a", "download_mode": "slow"}) == "[name=a]"

    def test_in_id_false_subsumes_elide_default(self):
        """When both flags are set, ``in_id=False`` wins and the axis
        never appears."""

        class V(AxesVariants):
            name = Axis(["a", "b"])
            flag = Axis(
                [False, True],
                default=False,
                type=bool,
                elide_default=True,
                in_id=False,
            )

        v = V()
        assert v.format_selector({"name": "a", "flag": True}) == "[name=a]"

    def test_in_id_false_still_reaches_resolve(self):
        """``resolve`` is unaffected — the value still flows through
        (so it can reach ``config(**kwargs)``, download hooks, etc.)."""

        class V(AxesVariants):
            download_mode = Axis(["fast", "slow"], default="fast", in_id=False)

        v = V()
        assert v.resolve() == {"download_mode": "fast"}
        assert v.resolve(download_mode="slow") == {"download_mode": "slow"}

    def test_all_axes_in_id_false_collapses_suffix(self):
        class V(AxesVariants):
            download_mode = Axis(["fast", "slow"], default="fast", in_id=False)
            retries = Axis(type=int, default=3, in_id=False)

        v = V()
        # No matter the values, the suffix is always empty.
        assert v.format_selector({}) == ""
        assert v.format_selector({"download_mode": "slow", "retries": 10}) == ""

    def test_mixed_elide_and_in_id_collapses_to_empty(self):
        """When some axes are elided-at-default and others are
        unconditionally hidden, and everything lines up, the whole
        suffix must collapse — no stray ``[]`` left behind."""

        class V(AxesVariants):
            streaming = Axis([False, True], default=True, type=bool, elide_default=True)
            download_mode = Axis(["fast", "slow"], default="fast", in_id=False)

        v = V()
        # Everything absent → no brackets.
        out = v.format_selector({})
        assert out == ""
        assert "[" not in out and "]" not in out
        # Still empty when the in_id=False axis takes a non-default
        # and the elidable axis stays at default.
        out = v.format_selector({"download_mode": "slow"})
        assert out == ""
        assert "[" not in out and "]" not in out

    def test_never_emits_empty_brackets(self):
        """Guard against a regression where ``format_selector`` might
        return the literal string ``"[]"``: brackets only appear when
        there's at least one key=value fragment inside."""

        class V(AxesVariants):
            a = Axis(["x"], default="x", elide_default=True)
            b = Axis(["y"], default="y", in_id=False)

        out = V().format_selector({})
        assert out != "[]"
        assert out == ""


class TestFormatSelectorDeterminism:
    """``format_selector`` must be invariant under input key order and
    always emit keys alphabetically — the dataset id depends on it."""

    def test_alphabetical_order_regardless_of_dict_insertion(self):
        class V(AxesVariants):
            zulu = Axis([1, 2], type=int)
            alpha = Axis(["x", "y"])
            mike = Axis([False, True], type=bool)

        v = V()
        a = v.format_selector({"zulu": 1, "alpha": "x", "mike": True})
        b = v.format_selector({"mike": True, "alpha": "x", "zulu": 1})
        assert a == b == "[alpha=x,mike=true,zulu=1]"

    def test_permuted_kwargs_give_identical_id(self):
        """Parse-of-format round-trip, with kwargs given in arbitrary
        orders — all must resolve to the same canonical string."""

        class V(AxesVariants):
            name = Axis(["a", "b"])
            streaming = Axis([False, True], default=True, type=bool)
            threshold = Axis(type=float, default=None)

        v = V()
        forms = [
            {"name": "a"},
            {"name": "a", "streaming": True},
            {"streaming": True, "name": "a"},
            {"name": "a", "streaming": True, "threshold": None},
            {"threshold": None, "streaming": True, "name": "a"},
        ]
        ids = {v.format_selector(f) for f in forms}
        assert len(ids) == 1


class TestIDBackwardCompat:
    """Regression guard for the ID-stability promise: adding an axis
    with ``elide_default=True`` + default MUST keep existing selectors
    formatting to their old id."""

    def test_new_elided_axis_preserves_existing_id(self):
        class VOld(AxesVariants):
            name = Axis(["a", "b"])

        class VNew(AxesVariants):
            name = Axis(["a", "b"])
            cache_mode = Axis(["mem", "disk"], default="mem", elide_default=True)

        old, new = VOld(), VNew()
        # Existing selector — same id on both.
        assert old.format_selector({"name": "a"}) == new.format_selector({"name": "a"})
        # New axis at non-default — appears only on the new family.
        assert (
            new.format_selector({"name": "a", "cache_mode": "disk"})
            == "[cache_mode=disk,name=a]"
        )

    def test_new_non_elided_axis_changes_id(self):
        """Adding an axis *without* ``elide_default`` *does* change the
        id — the escape hatch is opt-in."""

        class VOld(AxesVariants):
            name = Axis(["a"], default="a")

        class VNew(AxesVariants):
            name = Axis(["a"], default="a")
            extra = Axis(["x"], default="x")  # elide_default=False

        assert VOld().format_selector({}) != VNew().format_selector({})


# ---- enumerate ------------------------------------------------------------


class TestEnumerate:
    def test_cartesian_product(self):
        v = MyVariants()
        combos = list(v.enumerate())
        assert len(combos) == 2 * 2  # name * streaming (threshold is open)

    def test_enumerate_includes_open_axis_default(self):
        v = MyVariants()
        for combo in v.enumerate():
            # threshold is an open axis with default=None
            assert combo["threshold"] is None

    def test_enumerate_no_duplication_for_open_axes(self):
        v = MyVariants()
        combos = list(v.enumerate())
        # Uniqueness: (name, streaming) pairs
        pairs = {(c["name"], c["streaming"]) for c in combos}
        assert len(pairs) == 4

    def test_enumerate_empty_when_no_axes(self):
        class Empty(AxesVariants):
            pass

        v = Empty()
        combos = list(v.enumerate())
        # All axes fixed → yields one empty combo
        assert combos == [{}]


# ---- matches --------------------------------------------------------------


class TestMatches:
    def test_matches_enumerable_value(self):
        v = MyVariants()
        assert v.matches("agnews") is True
        assert v.matches("AGNEWS") is True  # case-insensitive

    def test_does_not_match_non_existing(self):
        v = MyVariants()
        assert v.matches("nonexistent") is False

    def test_skips_none_values(self):
        v = MyVariants()
        # threshold is None in every enumerated combo — should not match "None"
        assert v.matches("None") is False


# ---- Variants abstract contract ------------------------------------------


class PresetVariants(Variants):
    """Alternative non-axes implementation — demonstrates the contract is
    pluggable beyond cartesian products."""

    def __init__(self, presets: Dict[str, Dict[str, Any]]) -> None:
        self._presets = presets

    def resolve(self, **kwargs: Any) -> Dict[str, Any]:
        if set(kwargs) != {"preset"}:
            raise ValueError("PresetVariants expects exactly a 'preset' kwarg")
        name = kwargs["preset"]
        if name not in self._presets:
            raise ValueError(f"unknown preset: {name!r}")
        return {"preset": name, **self._presets[name]}

    def parse_selector(self, selector: str) -> Dict[str, Any]:
        body = (selector or "").strip().lstrip("[").rstrip("]").strip()
        if not body:
            return {}
        if "=" in body:
            k, v = body.split("=", 1)
            return {k.strip(): v.strip()}
        return {"preset": body}

    def format_selector(self, kwargs: Dict[str, Any]) -> str:
        resolved = self.resolve(**kwargs)
        return f"[preset={resolved['preset']}]"

    def enumerate(self) -> Iterator[Dict[str, Any]]:
        for name in self._presets:
            yield {"preset": name}


class TestAbstractContract:
    def test_preset_variants_implements_contract(self):
        pv = PresetVariants(
            {
                "small": {"batch": 8, "lr": 1e-4},
                "large": {"batch": 64, "lr": 1e-3},
            }
        )

        resolved = pv.resolve(preset="small")
        assert resolved == {"preset": "small", "batch": 8, "lr": 1e-4}

        assert pv.parse_selector("[small]") == {"preset": "small"}
        assert pv.parse_selector("[preset=small]") == {"preset": "small"}

        assert pv.format_selector({"preset": "large"}) == "[preset=large]"

        enumerated = list(pv.enumerate())
        assert enumerated == [{"preset": "small"}, {"preset": "large"}]

    def test_default_matches_uses_enumerate(self):
        pv = PresetVariants({"small": {}, "LARGE": {}})
        assert pv.matches("small") is True
        assert pv.matches("large") is True  # case-insensitive via default matches
        assert pv.matches("medium") is False

    def test_variants_cannot_be_instantiated_directly(self):
        # Variants is abstract — cannot instantiate.
        with pytest.raises(TypeError):
            Variants()  # type: ignore[abstract]
