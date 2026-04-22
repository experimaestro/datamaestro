# Dataset Variants

A dataset *family* can expose a set of variant axes that callers select at
prepare time. The selected combination is addressed with a query-style
suffix on the dataset id:

```
ai.lighton.embeddings_pre_training[name=agnews,streaming=true]
```

Unspecified axes fall back to declared defaults. Every resolved axis —
defaults included — participates in the experimaestro identity hash, so
caches stay disjoint per variant.

## Why variants?

Without variants, every concrete parameter combination needs its own
registered id. A family with 73 configs × two loading modes × four filter
flags blows up into hundreds of ids. Variants let you register *one*
family with declared axes and resolve specific combinations on demand —
`datamaestro prepare` understands the bracketed suffix out of the box, and
`datamaestro search` can match on axis values.

## The `Variants` contract

{py:class}`~datamaestro.variants.Variants` is an abstract description of a
dataset family's variant space. A concrete subclass implements four
methods:

- `resolve(**kwargs)` — validate incoming kwargs and fill in defaults.
- `parse_selector(selector)` — parse `"[k=v,k=v,…]"` into kwargs.
- `format_selector(kwargs)` — render the canonical `"[k=v,…]"` form.
  Includes every resolved value (defaults inclusive) for reproducibility.
- `enumerate()` — yield the finite variants for listing/expansion.

The default {py:meth}`~datamaestro.variants.Variants.matches` uses
`enumerate()` to support substring search over enumerable axis values.

Register alternative schemes (named presets, rule-based combinations, …)
by subclassing `Variants` directly.

```{eval-rst}
.. autoclass:: datamaestro.variants.Variants
    :members:
```

## `AxesVariants` — cartesian product of axes

{py:class}`~datamaestro.variants.AxesVariants` is the most common case:
one independent axis per dimension. Two construction styles, both
accepted anywhere ``Variants`` is:

**Declarative** — axes as class attributes:

```python
from datamaestro.variants import AxesVariants, Axis

class EmbeddingsVariants(AxesVariants):
    name             = Axis(CONFIGS)                       # required
    streaming        = Axis([False, True], default=True, type=bool)
    filter_drop      = Axis([True, False], default=True, type=bool)
    min_similarity   = Axis(type=float, default=None)      # open axis
```

**Imperative** — axes passed to the constructor:

```python
from datamaestro.variants import AxesVariants, Axis

variants = AxesVariants(
    name=Axis(CONFIGS),
    streaming=Axis([False, True], default=True, type=bool),
    min_similarity=Axis(type=float, default=None),
)
```

```{eval-rst}
.. autoclass:: datamaestro.variants.AxesVariants
    :members: axes, resolve, parse_selector, format_selector, enumerate, matches
```

## `Axis` — one dimension

{py:class}`~datamaestro.variants.Axis` declares a single variant
dimension.

- `Axis([v1, v2, ...])` — discrete enumerable domain; type is inferred
  from the values if they share one.
- `Axis(type=T)` — open axis; any value coercible to `T` is accepted.
  `T` may be `Optional[T_inner]`; then the literal `null`/`none` in a
  selector maps to `None`.
- `default=v` — value when the axis is omitted from a selector. If you
  don't set a default, the axis is required.

Supported coercions for selector strings:

| Type               | `"true"` / `"True"` → `True` | `"false"` / `"False"` → `False` | `"42"` → `int` | `"3.14"` → `float` | `"null"` → `None` |
|--------------------|:----------------------------:|:-------------------------------:|:--------------:|:------------------:|:-----------------:|
| `bool`             | ✔                            | ✔                               |                |                    |                   |
| `int`              |                              |                                 | ✔              |                    |                   |
| `float`            |                              |                                 | ✔              | ✔                  |                   |
| `str`              |                              |                                 |                |                    |                   |
| `Optional[T]`      | defers to `T`                | defers to `T`                   | defers to `T`  | defers to `T`      | ✔                 |

```{eval-rst}
.. autoclass:: datamaestro.variants.Axis
    :members: enumerable, has_default, coerce, validate
```

## Query-syntax reference

```
<dataset-id>[key1=value1,key2=value2,…]
```

- The brackets are literal; fragments separated by `,`.
- Whitespace around `=` and `,` is trimmed.
- Unknown keys raise `ValueError`.
- Absent axes fall back to declared defaults.
- An empty selector (`[]` or `[  ]`) resolves to all-defaults.

The canonical form emitted by `format_selector` always lists every axis
(defaults inclusive), sorted by axis name, so two equivalent selectors
render identically and round-trip through the id.

## Canonical id includes defaults

The user-facing form produced by `format_selector` includes every axis.
A user who types

```
ai.lighton.embeddings_pre_training[name=agnews]
```

sees back

```
ai.lighton.embeddings_pre_training[filter_drop=true,filter_duplicate=true,min_similarity=null,name=agnews,streaming=true,top_percentile=null]
```

as the identity-bearing canonical form. The short form is accepted as
shorthand; the canonical form is what drives the identity hash.
