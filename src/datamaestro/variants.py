"""Dataset variants.

A dataset *family* (e.g. `ai.lighton.embeddings_pre_training`) can expose
a set of variant axes that callers select at prepare time. The selected
combination is addressed with a query-style suffix on the id:

    ai.lighton.embeddings_pre_training[name=agnews,streaming=true]

Unspecified axes fall back to declared defaults. Every resolved axis —
defaults included — participates in the downstream experimaestro identity
hash, so caches stay disjoint per variant.

``Variants`` is the abstract contract. ``AxesVariants`` implements the
cartesian-product case (one independent axis per dimension). Other
schemes (e.g. named presets) can implement ``Variants`` directly.
"""

from __future__ import annotations

import itertools
import typing
from abc import ABC, abstractmethod
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Mapping, Optional


class _Missing:
    """Sentinel for "no default provided" so ``None`` stays a valid default."""

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "MISSING"


MISSING: Any = _Missing()


class Axis:
    """One dimension of a variant space.

    Args:
        domain: Either a concrete list of allowed values (discrete,
            enumerable axis) or ``None`` (open axis — any value coerced
            via ``type`` is accepted).
        type: Explicit Python type used to coerce selector strings. If
            omitted, inferred from the domain when homogeneous. Supports
            ``str``, ``int``, ``float``, ``bool``, and
            ``Optional[T]``. For open axes without ``type``, values are
            kept as strings.
        default: Value returned when the axis is not set in a selector.
            Omit to mark the axis as required.
        description: Optional human-readable label (used by search UI).
    """

    __slots__ = ("domain", "type", "default", "description")

    def __init__(
        self,
        domain: Optional[List[Any]] = None,
        *,
        type: Optional[Any] = None,
        default: Any = MISSING,
        description: str = "",
    ) -> None:
        if domain is not None and not isinstance(domain, list):
            raise TypeError(
                f"Axis domain must be a list of values or None (got {domain!r})"
            )
        self.domain = list(domain) if domain is not None else None
        self.type = type if type is not None else self._infer_type(self.domain)
        self.default = default
        self.description = description

    @staticmethod
    def _infer_type(domain: Optional[List[Any]]) -> Optional[Any]:
        import builtins

        if not domain:
            return None
        types = {builtins.type(v) for v in domain if v is not None}
        if len(types) == 1:
            return next(iter(types))
        return None

    @property
    def enumerable(self) -> bool:
        """True iff the axis has a finite enumerated domain."""
        return self.domain is not None

    @property
    def has_default(self) -> bool:
        return self.default is not MISSING

    def coerce(self, raw: Any) -> Any:
        """Coerce a value (typically a string from query syntax) using
        the axis' declared type.

        Returns the raw value unchanged if no type is known and no
        special marker is detected.
        """
        if raw is None:
            return None
        if isinstance(raw, str) and raw.lower() in ("null", "none"):
            return None

        t = self.type
        if t is None:
            return raw

        origin = typing.get_origin(t)
        args = typing.get_args(t)
        if origin is typing.Union and type(None) in args:
            inner = next((a for a in args if a is not type(None)), None)
            if inner is None:
                return raw
            return self._coerce_to(raw, inner)
        return self._coerce_to(raw, t)

    @staticmethod
    def _coerce_to(raw: Any, t: Any) -> Any:
        if isinstance(raw, t):
            return raw
        if t is str:
            return str(raw)
        if t is bool:
            if isinstance(raw, str):
                low = raw.lower()
                if low in ("true", "yes", "1"):
                    return True
                if low in ("false", "no", "0"):
                    return False
                raise ValueError(f"cannot coerce {raw!r} to bool")
            return bool(raw)
        if t is int:
            return int(raw)
        if t is float:
            return float(raw)
        # Last-resort: try the constructor
        return t(raw)

    def validate(self, value: Any) -> None:
        """Raise ``ValueError`` if ``value`` is outside an enumerable
        domain. Open axes accept any value."""
        if self.enumerable and value not in self.domain:
            raise ValueError(f"value {value!r} not in axis domain {self.domain!r}")

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        parts = []
        if self.domain is not None:
            parts.append(f"domain={self.domain!r}")
        if self.type is not None:
            parts.append(f"type={self.type!r}")
        if self.has_default:
            parts.append(f"default={self.default!r}")
        return "Axis(" + ", ".join(parts) + ")"


class Variants(ABC):
    """Abstract description of a dataset family's variant space.

    Subclasses define how a selector string translates to concrete kwargs
    passed into a dataset factory. The most common case is
    ``AxesVariants`` (cartesian product of independent axes); other
    schemes (named presets, rule-based combinations) can implement this
    contract directly.
    """

    @abstractmethod
    def resolve(self, **kwargs: Any) -> Dict[str, Any]:
        """Validate ``kwargs`` and return the full canonical variant.

        The returned dict MUST include every axis (with defaults filled
        in) so that it participates in the downstream identity hash.
        Raise ``ValueError`` for unknown axes or missing required axes.
        """

    @abstractmethod
    def parse_selector(self, selector: str) -> Dict[str, Any]:
        """Parse a ``"[k=v,k=v]"`` selector into a kwargs dict.

        Values are coerced using the axis' declared type. An empty
        selector (``""`` or ``"[]"``) returns ``{}``.
        """

    @abstractmethod
    def format_selector(self, kwargs: Dict[str, Any]) -> str:
        """Render a canonical ``"[k=v,...]"`` string for ``kwargs``.

        Implementations MUST include every resolved value (defaults
        inclusive) so two equivalent selectors format identically and
        round-trip through the id.
        """

    @abstractmethod
    def enumerate(self) -> Iterator[Dict[str, Any]]:
        """Yield the finite variants (if any) for listing/expansion."""

    def matches(self, query: str) -> bool:
        """True if ``query`` is a substring of any enumerable axis
        value. Used by ``datamaestro search``; subclasses can override."""
        q = query.lower()
        for combo in self.enumerate():
            for value in combo.values():
                if value is None:
                    continue
                if q in str(value).lower():
                    return True
        return False


class AxesVariants(Variants):
    """Cartesian product of independent axes.

    Two construction styles, both accepted anywhere ``Variants`` is:

    Declarative (recommended for readability)::

        class MyVariants(AxesVariants):
            name = Axis(["small", "large"])
            streaming = Axis([False, True], default=True, type=bool)

    Imperative::

        v = AxesVariants(
            name=Axis(["small", "large"]),
            streaming=Axis([False, True], default=True, type=bool),
        )
    """

    # Populated by ``__init_subclass__`` for declarative subclasses.
    _class_axes: ClassVar[Dict[str, Axis]] = {}

    def __init_subclass__(cls, **kw: Any) -> None:
        super().__init_subclass__(**kw)
        collected: Dict[str, Axis] = {}
        # Walk MRO so inherited axes carry over, with subclass overrides.
        for base in reversed(cls.__mro__):
            for key, value in vars(base).items():
                if isinstance(value, Axis):
                    collected[key] = value
        cls._class_axes = collected

    def __init__(self, **axes: Axis) -> None:
        for key, value in axes.items():
            if not isinstance(value, Axis):
                raise TypeError(
                    f"axis {key!r} must be an Axis instance (got {value!r})"
                )
        merged: Dict[str, Axis] = {**type(self)._class_axes, **axes}
        self._axes: Dict[str, Axis] = merged

    @property
    def axes(self) -> Mapping[str, Axis]:
        """Read-only view of the axes declared on this variant space."""
        return dict(self._axes)

    def resolve(self, **kwargs: Any) -> Dict[str, Any]:
        unknown = [k for k in kwargs if k not in self._axes]
        if unknown:
            raise ValueError(
                f"unknown variant axes: {unknown!r}; known axes: {list(self._axes)!r}"
            )
        result: Dict[str, Any] = {}
        for key, axis in self._axes.items():
            if key in kwargs:
                axis.validate(kwargs[key])
                result[key] = kwargs[key]
            elif axis.has_default:
                result[key] = axis.default
            else:
                raise ValueError(f"missing required variant axis: {key!r}")
        return result

    def parse_selector(self, selector: str) -> Dict[str, Any]:
        body = (selector or "").strip()
        if body.startswith("[") and body.endswith("]"):
            body = body[1:-1]
        body = body.strip()
        if not body:
            return {}

        out: Dict[str, Any] = {}
        for part in self._split_csv(body):
            if "=" not in part:
                raise ValueError(
                    f"malformed selector fragment: {part!r} (expected 'key=value')"
                )
            key, raw = part.split("=", 1)
            key, raw = key.strip(), raw.strip()
            if key not in self._axes:
                raise ValueError(
                    f"unknown variant axis: {key!r}; known axes: {list(self._axes)!r}"
                )
            out[key] = self._axes[key].coerce(raw)
        return out

    @staticmethod
    def _split_csv(body: str) -> List[str]:
        """Split on commas, respecting nothing fancy (no quoting needed
        for current domain values). Empty fragments are ignored."""
        return [p.strip() for p in body.split(",") if p.strip()]

    def format_selector(self, kwargs: Dict[str, Any]) -> str:
        resolved = self.resolve(**kwargs)
        parts = [
            f"{key}={self._format_value(resolved[key])}" for key in sorted(resolved)
        ]
        return "[" + ",".join(parts) + "]"

    @staticmethod
    def _format_value(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def enumerate(self) -> Iterator[Dict[str, Any]]:
        """Cartesian product over enumerable axes.

        Open axes (``domain=None``) with a default contribute that
        default; open axes without a default are omitted (they cannot
        be expanded).
        """
        enumerable_keys: List[str] = []
        enumerable_choices: List[List[Any]] = []
        fixed: Dict[str, Any] = {}
        for key, axis in self._axes.items():
            if axis.enumerable:
                enumerable_keys.append(key)
                enumerable_choices.append(list(axis.domain or []))
            elif axis.has_default:
                fixed[key] = axis.default

        if not enumerable_keys:
            yield dict(fixed)
            return

        for combo in itertools.product(*enumerable_choices):
            item = dict(fixed)
            for k, v in zip(enumerable_keys, combo):
                item[k] = v
            yield item


# Callable matching the factory signature expected by the registration
# surface. Kept here so downstream callers can type-annotate cleanly.
VariantFactory = Callable[..., Any]


def split_id_selector(query: str) -> tuple[str, str]:
    """Split a query like ``"id[selector]"`` into ``("id", "selector")``.

    Returns ``(query, "")`` when no bracket suffix is present. Trailing/
    leading whitespace is stripped from the base id but selectors are
    returned as-is (callers pass them to ``Variants.parse_selector``).
    """
    q = query.strip()
    if "[" in q and q.endswith("]"):
        base, rest = q.split("[", 1)
        return base.strip(), rest[:-1]
    return q, ""


__all__ = [
    "MISSING",
    "Axis",
    "Variants",
    "AxesVariants",
    "VariantFactory",
    "split_id_selector",
]
