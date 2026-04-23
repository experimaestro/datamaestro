# Sphinx extension for datamaestro datasets

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from sphinx.ext.autodoc.mock import mock

from docutils import nodes
from docutils.statemachine import StringList

from sphinx.application import Sphinx
from sphinx.domains import Domain, ObjType
from sphinx.roles import XRefRole
from sphinx.util.docutils import SphinxDirective
from sphinx.locale import _
from sphinx import addnodes
from sphinx.util.nodes import make_refnode
import datamaestro
from datamaestro.data import AbstractDataset
import logging

_HERE = Path(__file__).parent
_STATIC_DIR = _HERE / "_static"
# CDN-hosted MiniSearch (UMD build, exposes a global `MiniSearch`).
_MINISEARCH_CDN = "https://cdn.jsdelivr.net/npm/minisearch@7.1.2/dist/umd/index.min.js"


class DatasetNode(nodes.paragraph):
    pass


class DatasetsDirective(SphinxDirective):
    def _parse_rst(self, source: str) -> List[nodes.Node]:
        """Parse ``source`` as reStructuredText using the directive's
        state machine so Sphinx roles (``:class:``, ``:func:``, ...) resolve
        against the ``py`` domain."""
        container = nodes.Element()
        self.state.nested_parse(
            StringList(source.splitlines(), source="<datamaestro>"),
            0,
            container,
        )
        return list(container.children)

    def dataset_desc(self, ds: AbstractDataset):
        dm = self.env.get_domain("dm")

        assert isinstance(dm, DatamaestroDomain)
        dm.add_dataset(ds.id, record=_dataset_record(ds, self.env.docname))

        # indexnode = addnodes.index(entries=[])
        desc = addnodes.desc()
        desc["domain"] = DatamaestroDomain.name
        desc["objtype"] = desc["desctype"] = "dataset"
        desc["classes"].append(DatamaestroDomain.name)

        anchor = "dataset-" + ds.id
        signodes = addnodes.desc_signature(ds.id, "", is_multiline=True)
        # Sphinx's HTML translator reads ``ids`` off the desc_signature
        # (the ``<dt>``) — putting it only on desc_signature_line means
        # the anchor never appears in the HTML.
        signodes["ids"].append(anchor)
        desc.append(signodes)

        signode = addnodes.desc_signature_line()
        signode += nodes.Text("Dataset ")
        signode += addnodes.desc_name(text=ds.id)
        signodes.append(signode)

        content = addnodes.desc_content()
        desc.append(content)

        if ds.configtype:
            ctype = ds.configtype
            name = f"{ctype.__module__}.{ctype.__qualname__}"

            te = nodes.paragraph()
            te.append(nodes.Text("Experimaestro type: "))

            p = nodes.paragraph()
            returns = addnodes.desc_returns()
            xref = addnodes.pending_xref(
                "",
                nodes.Text(name),
                refdomain="py",
                reftype="class",
                reftarget=name,
            )
            returns.append(xref)
            p.append(returns)

            content.append(p)

        # node.append(nodes.Text(ds.id))
        if ds.name:
            content.append(
                nodes.paragraph("", "", nodes.strong("", nodes.Text(ds.name)))
            )

        if ds.tags or ds.tasks:
            if ds.tags:
                content.append(
                    nodes.paragraph(
                        "",
                        "",
                        nodes.strong("", nodes.Text("Tags: ")),
                        nodes.Text(", ".join(ds.tags)),
                    )
                )
            if ds.tasks:
                content.append(
                    nodes.paragraph(
                        "",
                        "",
                        nodes.strong("", "Tasks: "),
                        nodes.Text(", ".join(ds.tasks)),
                    )
                )

        if ds.url:
            href = nodes.reference(refuri=ds.url)
            href.append(nodes.Text(ds.url))
            p = nodes.paragraph()
            p.append(nodes.Text("External link: "))
            p.append(href)
            content.append(p)

        if ds.description:
            content.extend(self._parse_rst(ds.description))

        variants = getattr(ds, "variants", None)
        if variants is not None:
            variant_doc = variants.document()
            if variant_doc:
                content.extend(self._parse_rst(variant_doc))

        return desc


class RepositoryDirective(DatasetsDirective):
    """Generates the document for a whole repository"""

    has_content = True
    required_arguments = 1
    optional_arguments = 0

    def run(self):
        (repository_id,) = self.arguments
        with mock(self.config.autodoc_mock_imports):
            repository = datamaestro.Context.instance().repository(repository_id)  # type: Optional[datamaestro.Repository]
            assert repository is not None

            docnodes = []
            for module in repository.modules():
                section = nodes.section(
                    ids=[f"dm-datasets-{repository_id}-{module.id}"]
                )
                docnodes.append(section)

                section += nodes.title("", nodes.Text(module.title))
                section += nodes.paragraph()
                if module.description:
                    section += self._parse_rst(module.description)

                for ds in iter(module):
                    section += self.dataset_desc(ds)

        return docnodes


class DatasetDirective(DatasetsDirective):
    has_content = True
    required_arguments = 1
    optional_arguments = 1

    def run(self):
        # --- Retrieve the datasets
        if len(self.arguments) == 2:
            module_name, repository_name = self.arguments
        else:
            (module_name,) = self.arguments
            repository_name = self.env.config["datamaestro_repository"]

        datasets = None
        with mock(self.config.autodoc_mock_imports):
            for repository in datamaestro.Context.instance().repositories():
                if repository_name is None or repository.id == repository_name:
                    datasets = repository.datasets(module_name)
                    if datasets is not None:
                        break

        assert datasets is not None

        # --- Start documenting

        # Wrap the module in its own section so the module docstring
        # (title + description) has a proper heading above it instead of
        # appearing abruptly before the first dataset entry.
        section_id = f"dm-datasets-{repository_name or 'repo'}-{module_name}"
        section = nodes.section(ids=[section_id])
        title_text = datasets.title or module_name
        section += nodes.title("", nodes.Text(title_text))
        if datasets.description:
            section += self._parse_rst(datasets.description)

        for ds in datasets:
            section += self.dataset_desc(ds)
        return [section]


def _json_safe(value: Any) -> Any:
    """Best-effort conversion of arbitrary Python values to JSON-friendly
    primitives. Falls back to ``repr`` for opaque objects so the index
    write never crashes the Sphinx build."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    return repr(value)


def _pretty_axis_type(t: Any) -> Optional[str]:
    if t is None:
        return None
    return getattr(t, "__name__", None) or repr(t)


def _variants_record(ds: AbstractDataset) -> Optional[Dict[str, Any]]:
    variants = getattr(ds, "variants", None)
    if variants is None:
        return None
    record: Dict[str, Any] = {}
    try:
        doc = variants.document() or ""
    except Exception:
        doc = ""
    if doc:
        record["description"] = doc
    axes_attr = getattr(variants, "axes", None)
    if axes_attr:
        # PEP 224-style attribute docstrings live on the AxesVariants
        # subclass, not on the Axis instance — pull them in so the
        # search UI shows the same per-axis text the docs page does.
        try:
            from datamaestro.variants import _axis_attr_docs

            attr_docs = _axis_attr_docs(type(variants))
        except Exception:
            attr_docs = {}

        axes: Dict[str, Any] = {}
        try:
            items = axes_attr.items()
        except AttributeError:
            items = []
        for key, axis in items:
            description = attr_docs.get(key) or getattr(axis, "description", "") or ""
            axes[key] = {
                "type": _pretty_axis_type(getattr(axis, "type", None)),
                "domain": _json_safe(getattr(axis, "domain", None)),
                "default": (
                    _json_safe(axis.default)
                    if getattr(axis, "has_default", False)
                    else None
                ),
                "has_default": bool(getattr(axis, "has_default", False)),
                "description": description,
                "elide_default": bool(getattr(axis, "elide_default", False)),
                "in_id": bool(getattr(axis, "in_id", True)),
            }
        if axes:
            record["axes"] = axes
    return record or None


def _dataset_record(ds: AbstractDataset, docname: str) -> Dict[str, Any]:
    """Build the JSON-serializable record stored in the search index for
    ``ds``. Catches per-attribute failures so an individual dataset never
    blocks indexing of the rest."""
    try:
        ctype = ds.configtype
        ctype_name = (
            f"{ctype.__module__}.{ctype.__qualname__}" if ctype is not None else None
        )
    except Exception:
        ctype_name = None

    try:
        name = ds.name or ""
    except Exception:
        name = ""

    try:
        description = ds.description or ""
    except Exception:
        description = ""

    return {
        "id": ds.id,
        "name": name,
        "tags": sorted(ds.tags) if getattr(ds, "tags", None) else [],
        "tasks": sorted(ds.tasks) if getattr(ds, "tasks", None) else [],
        "description": description,
        "url": getattr(ds, "url", None) or "",
        "configtype": ctype_name,
        "variants": _variants_record(ds),
        "docname": docname,
        "anchor": f"dataset-{ds.id}",
    }


class SearchDirective(SphinxDirective):
    """Embed the JS-backed dataset search widget on the current page.

    Usage::

        .. dm:search::
    """

    has_content = False
    optional_arguments = 0

    def run(self):
        # Climb back to the docs root so result hrefs work from any
        # nesting depth.
        depth = self.env.docname.count("/")
        url_root = "../" * depth if depth else "./"
        html = (
            f'<div class="dm-search" data-url-root="{url_root}">'
            '<input type="search" class="dm-search-input" '
            'placeholder="Search datasets by name, id, tag, or task…" '
            'aria-label="Search datasets" autocomplete="off" />'
            '<div class="dm-search-stats" aria-live="polite"></div>'
            '<div class="dm-search-layout">'
            '<ul class="dm-search-results" role="listbox"></ul>'
            '<div class="dm-search-details" aria-live="polite">'
            '<p class="dm-search-placeholder">'
            "Type a query to search; click a result to see details."
            "</p>"
            "</div>"
            "</div>"
            "</div>"
        )
        return [nodes.raw("", html, format="html")]


class DatamaestroDomain(Domain):
    name = "dm"
    object_types = {
        "dataset": ObjType(_("dataset"), "ds"),
    }
    directives = {
        "repository": RepositoryDirective,
        "datasets": DatasetDirective,
        "search": SearchDirective,
    }
    roles = {"ref": XRefRole()}
    indices = {
        # TODO: Add indices for tags and tasks
    }
    initial_data: Dict[str, Any] = {
        "datasets": {},  # fullname -> (docname, anchor)
        "records": {},  # fullname -> JSON record (search index input)
        "tags": {},  # tag  -> list of datasets,
        "tasks": {},  # task name -> list of datasets
    }

    def add_dataset(self, dsid, record: Optional[Dict[str, Any]] = None):
        self.data["datasets"][dsid] = (self.env.docname, f"dataset-{dsid}")
        if record is not None:
            self.data["records"][dsid] = record

    def resolve_xref(self, env, fromdocname, builder, typ, target, node, contnode):
        logging.debug("[dm/sphinx] Searching for", target)

        ref = self.data["datasets"].get(target, None)
        if ref:
            docname, targ = ref
            return make_refnode(builder, fromdocname, docname, targ, contnode, targ)
        return None

    def merge_domaindata(self, docnames, otherdata):
        # Required for parallel reads — merge per-process domain state.
        for key in ("datasets", "records", "tags", "tasks"):
            self.data.setdefault(key, {}).update(otherdata.get(key, {}))


def _write_search_index(app: Sphinx, exception: Optional[Exception]) -> None:
    """Dump the collected dataset records as both ``_static/datasets.json``
    (handy for external consumers / HTTP-served docs) and
    ``_static/datasets.js`` (which just assigns the data to a global so it
    can be loaded via a ``<script>`` tag — required for browsing the built
    docs over ``file://`` since ``fetch`` is CORS-blocked there).

    Skipped on failed builds so we never publish a half-baked index."""
    if exception is not None:
        return
    if not hasattr(app, "env"):
        return
    try:
        domain = app.env.get_domain("dm")
    except Exception:
        return
    records = list(domain.data.get("records", {}).values())
    out_dir = Path(app.outdir) / "_static"
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(records, default=str)
    (out_dir / "datasets.json").write_text(payload, encoding="utf-8")
    (out_dir / "datasets.js").write_text(
        "window.__DATAMAESTRO_DATASETS__ = " + payload + ";\n",
        encoding="utf-8",
    )


def _register_static_path(app: Sphinx) -> None:
    """Append our bundled ``_static`` directory once the builder is up.
    Done in ``builder-inited`` (not ``setup``) because ``html_static_path``
    isn't reliably populated at extension-setup time."""
    static = str(_STATIC_DIR)
    if not _STATIC_DIR.is_dir():
        return
    paths = getattr(app.config, "html_static_path", None)
    if paths is None:
        return
    if static not in paths:
        paths.append(static)


def setup(app: Sphinx) -> Dict[str, Any]:
    """Setup experimaestro for Sphinx documentation"""

    app.add_domain(DatamaestroDomain)
    app.add_node(DatasetNode)

    app.add_config_value("datamaestro_repository", None, True)

    # Ship search assets alongside the extension.
    app.add_css_file("dataset-search.css")
    app.add_js_file(_MINISEARCH_CDN)
    app.add_js_file("dataset-search.js")

    app.connect("builder-inited", _register_static_path)
    app.connect("build-finished", _write_search_index)

    return {"version": datamaestro.version, "parallel_read_safe": True}
