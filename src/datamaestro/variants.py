"""Dataset variants.

A dataset *family* (e.g. `ai.lighton.embeddings_pre_training`) can expose
a set of variant axes that callers select at prepare time. The selected
combination is addressed with a query-style suffix on the id:

    ai.lighton.embeddings_pre_training[name=agnews,streaming=true]

Unspecified axes fall back to declared defaults. The full resolved dict
(defaults included) is what downstream code — e.g. the experimaestro
config constructor — consumes, so caches stay disjoint per variant.

Dataset ID computation
----------------------

For a variant family, the dataset's ``Base.id`` is built as:

    variant_id = wrapper.id + variants.format_selector(resolved_kwargs)

``AxesVariants.format_selector`` produces a canonical, deterministic
suffix:

1. Keys are emitted in alphabetical order (so two selectors that resolve
   to the same kwargs format identically).
2. Every axis appears by default, *including* axes left at their default
   value, which keeps the id unambiguous about what was asked for.
3. An axis marked ``elide_default=True`` is *omitted* from the suffix
   when its resolved value equals the declared default. If every axis
   elides, the whole ``[...]`` suffix is dropped — the family id becomes
   identical to a flat (non-variant) id of the same name.
4. An axis marked ``in_id=False`` is *always* omitted from the suffix,
   regardless of its value. Use this for flags that don't change the
   dataset output (e.g. a download-time behavior flag). Caveat: two
   prepared configs that differ only on such an axis share an id but
   may be distinct Python objects — don't use ``in_id=False`` for
   axes that feed into :meth:`Dataset.config` in a way that affects
   the built object.

Rules (3) and (4) are how a family can gain new axes without breaking
existing ids: declare the new axis with ``elide_default=True`` (or
``in_id=False`` if the axis should never participate) and a default,
and pre-existing selectors continue to format to the same string.

``Variants`` is the abstract contract. ``AxesVariants`` implements the
cartesian-product case (one independent axis per dimension). Other
schemes (e.g. named presets) can implement ``Variants`` directly; they
are responsible for their own canonicalization and elision policy.
"""

from __future__ import annotations

import ast
import inspect
import itertools
import textwrap
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
        elide_default: When True, omit this axis from the formatted
            selector (and therefore from the dataset id suffix) whenever
            its resolved value equals ``default``. Lets a family grow
            new axes without changing the id of already-prepared
            variants. Requires ``default`` to be set.
        in_id: When False, this axis is *always* excluded from the
            formatted selector (and thus from the dataset id). The axis
            still participates in :meth:`AxesVariants.resolve` and in
            the cache key, so its value still reaches
            :meth:`Dataset.config`. Use for download-time flags (or
            similar) whose value doesn't change the output config.
            Subsumes ``elide_default`` — when ``in_id=False`` the axis
            is omitted regardless of the value.
    """

    __slots__ = (
        "domain",
        "type",
        "default",
        "description",
        "elide_default",
        "in_id",
    )

    def __init__(
        self,
        domain: Optional[List[Any]] = None,
        *,
        type: Optional[Any] = None,
        default: Any = MISSING,
        description: str = "",
        elide_default: bool = False,
        in_id: bool = True,
    ) -> None:
        if domain is not None and not isinstance(domain, list):
            raise TypeError(
                f"Axis domain must be a list of values or None (got {domain!r})"
            )
        if elide_default and default is MISSING:
            raise ValueError("Axis(elide_default=True) requires a default value")
        self.domain = list(domain) if domain is not None else None
        self.type = type if type is not None else self._infer_type(self.domain)
        self.default = default
        self.description = description
        self.elide_default = elide_default
        self.in_id = in_id

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

    def document(self) -> str:
        """Return a Markdown description of this variant space.

        Base behaviour is to emit the subclass's own docstring (if any).
        Specialized ``Variants`` subclasses should override this and may
        call ``super().document()`` to chain onto the docstring.

        The returned string is rendered by documentation consumers
        (e.g. the Sphinx ``dm:datasets`` directive) through a Markdown
        parser; keep the output framework-agnostic.
        """
        own_doc = type(self).__doc__
        if own_doc is None:
            return ""
        return inspect.cleandoc(own_doc)


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
        """Canonical string form of ``kwargs``.

        Keys are emitted alphabetically (deterministic). Axes declared
        with ``in_id=False`` are always omitted. Axes declared with
        ``elide_default=True`` are omitted when their resolved value
        equals the default. If every axis ends up omitted, returns
        ``""`` so the dataset id suffix disappears entirely.
        """
        resolved = self.resolve(**kwargs)
        parts: List[str] = []
        for key in sorted(resolved):
            axis = self._axes[key]
            if not axis.in_id:
                continue
            value = resolved[key]
            if axis.elide_default and axis.has_default and value == axis.default:
                continue
            parts.append(f"{key}={self._format_value(value)}")
        if not parts:
            return ""
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

    def document(self) -> str:
        """reStructuredText description of the variant space.

        Emits the subclass's own docstring followed by a bullet list of
        axes. Each axis line shows its name, type (when known), default,
        domain (when enumerable and small), and ``in_id`` / ``elide_default``
        markers. Per-axis descriptions come from — in priority order —
        PEP 224-style attribute docstrings on the ``AxesVariants``
        subclass, then ``Axis.description``.

        Output is reST so cross-reference roles (``:class:``, ``:func:``,
        ...) and other reST markup in the source docstrings resolve
        correctly against the Sphinx ``py`` domain.

        Override on a further subclass to add extra context, then call
        ``super().document()`` to keep the generated axis listing.
        """
        parts: List[str] = []
        own_doc = type(self).__doc__
        if own_doc is not None:
            cleaned = inspect.cleandoc(own_doc).strip()
            if cleaned:
                parts.append(cleaned)

        if not self._axes:
            return "\n\n".join(parts)

        attr_docs = _axis_attr_docs(type(self))

        lines: List[str] = ["**Variants**:", ""]
        for key, axis in self._axes.items():
            header = f"- ``{key}``"
            type_str = _pretty_type(axis.type)
            if type_str:
                header += f" : ``{type_str}``"
            extras = _axis_extras(axis)
            if extras:
                header += f" *({'; '.join(extras)})*"
            lines.append(header)

            doc_text = attr_docs.get(key) or axis.description
            if doc_text:
                # Blank line separates the bullet from its continuation
                # paragraph; each continuation line must be indented.
                lines.append("")
                for line in inspect.cleandoc(doc_text).splitlines():
                    lines.append(f"  {line}" if line else "")
        parts.append("\n".join(lines))
        return "\n\n".join(parts)


def _axis_attr_docs(variants_cls: type) -> Dict[str, str]:
    """Extract PEP 224-style attribute docstrings for Axis fields on
    ``variants_cls`` and its bases.

    Python doesn't preserve string literals that follow an attribute
    assignment, so we re-parse the class source with ``ast`` to recover
    them. Walks the MRO so inherited axis docstrings propagate, with
    subclass declarations overriding.
    """
    docs: Dict[str, str] = {}
    for cls in reversed(variants_cls.__mro__):
        if cls is object:
            continue
        try:
            src = inspect.getsource(cls)
        except (OSError, TypeError):
            continue
        try:
            tree = ast.parse(textwrap.dedent(src))
        except SyntaxError:
            continue
        if not tree.body or not isinstance(tree.body[0], ast.ClassDef):
            continue
        body = tree.body[0].body
        for idx, node in enumerate(body):
            name: Optional[str] = None
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                name = node.targets[0].id
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                name = node.target.id
            if name is None or idx + 1 >= len(body):
                continue
            nxt = body[idx + 1]
            if (
                isinstance(nxt, ast.Expr)
                and isinstance(nxt.value, ast.Constant)
                and isinstance(nxt.value.value, str)
            ):
                docs[name] = inspect.cleandoc(nxt.value.value)
    return docs


def _pretty_type(t: Any) -> str:
    if t is None:
        return ""
    origin = typing.get_origin(t)
    if origin is typing.Union:
        args = typing.get_args(t)
        if type(None) in args:
            inner = next((a for a in args if a is not type(None)), None)
            return f"Optional[{_pretty_type(inner)}]" if inner else "Optional"
        return " | ".join(_pretty_type(a) for a in args)
    if origin is not None:
        args = typing.get_args(t)
        base = getattr(origin, "__name__", repr(origin))
        return f"{base}[{', '.join(_pretty_type(a) for a in args)}]"
    return getattr(t, "__name__", repr(t))


def _axis_extras(axis: Axis) -> List[str]:
    parts: List[str] = []
    if axis.has_default:
        parts.append(f"default={axis.default!r}")
    if axis.domain is not None:
        if len(axis.domain) <= 6:
            parts.append(f"domain={axis.domain!r}")
        else:
            parts.append(f"domain: {len(axis.domain)} values")
    if not axis.in_id:
        parts.append("excluded from id")
    elif axis.elide_default:
        parts.append("elides default")
    return parts


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
