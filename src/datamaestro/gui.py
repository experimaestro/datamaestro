"""Web interface to browse the datasets (``datamaestro serve``)

Requires the ``gui`` extra (``pip install datamaestro[gui]``), which
installs `NiceGUI <https://nicegui.io>`_.

The datasets come from the :class:`~datamaestro.catalog.Catalog`, and their
download status from the ds-cache-cleaner registry
(:mod:`datamaestro.cleaner`).
"""

import re
from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple

from datamaestro.catalog import Catalog, DatasetEntry

if TYPE_CHECKING:
    from ds_cache_cleaner import EntryMetadata

    from datamaestro.context import Context

FACETS = [
    ("status", "Status"),
    ("repository", "Repositories"),
    ("task", "Tasks"),
    ("tag", "Tags"),
]
"""Facets shown in the side panel: (field, title)"""

MAX_FACET_VALUES = 30
"""Maximum number of values shown for a facet"""


# --- Helpers (no NiceGUI needed)


DOWNLOADED = "downloaded"
NOT_DOWNLOADED = "not downloaded"
UNAVAILABLE = "not in this environment"
"""Downloaded, but not defined in the current Python environment"""


def status(entry: DatasetEntry, downloads: Dict[str, "EntryMetadata"]) -> str:
    if not entry.available:
        return UNAVAILABLE
    return DOWNLOADED if entry.path in downloads else NOT_DOWNLOADED


def make_query(text: str, filters: Iterable[Tuple[str, str]]) -> List[str]:
    """Search terms for a free text and selected facet values (the status
    facet is not part of the query)"""
    terms = text.split()
    for name, value in sorted(filters):
        if name == "status":
            continue
        terms.append(f"{name}:^{re.escape(value)}$")
    return terms


def variant_id(entry: DatasetEntry, values: Dict[str, Optional[str]], dataset=None):
    """Canonical id of a variant

    :param values: Selected value (selector form) of the axes; None or empty
        values are left unset (so the default is used)
    :param dataset: The dataset (loaded from the entry if not given)
    :return: a pair (id, error message)
    """
    selector = ",".join(f"{key}={value}" for key, value in values.items() if value)
    try:
        variants = (dataset or entry.dataset()).variants
        kwargs = variants.parse_selector(selector)
        return f"{entry.id}{variants.format_selector(kwargs)}", None
    except Exception as e:
        return None, str(e)


def human_size(size: Optional[int]) -> str:
    if size is None:
        return ""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024


def _date(value: datetime) -> str:
    return value.astimezone().strftime("%Y-%m-%d %H:%M")


def rows(
    entries: Iterable[DatasetEntry], downloads: Dict[str, "EntryMetadata"]
) -> List[dict]:
    """Table rows"""
    result = []
    for entry in entries:
        download = downloads.get(entry.path) if entry.path else None
        result.append(
            {
                "id": entry.id,
                "variants": (
                    ""
                    if entry.variants is None
                    else ",".join(axis.name for axis in entry.variants.axes or [])
                    or "…"
                ),
                "name": entry.name,
                "repository": entry.repository or "",
                "tags": ", ".join(entry.tags),
                "size": download.size if download else None,
                "size_label": human_size(download.size) if download else "",
                "status": "" if entry.available else UNAVAILABLE,
            }
        )
    return result


COLUMNS = [
    {"name": "id", "label": "Id", "field": "id", "sortable": True, "align": "left"},
    {"name": "variants", "label": "Variants", "field": "variants", "align": "left"},
    {"name": "name", "label": "Name", "field": "name", "align": "left"},
    {
        "name": "repository",
        "label": "Repository",
        "field": "repository",
        "sortable": True,
        "align": "left",
    },
    {"name": "tags", "label": "Tags", "field": "tags", "align": "left"},
    {
        "name": "size",
        "label": "Downloaded",
        "field": "size",
        "sortable": True,
        ":format": "(value, row) => row.size_label",
    },
    {"name": "status", "label": "", "field": "status", "align": "left"},
]


# --- Web interface


class Browser:
    """The interface of one client (browser tab)"""

    def __init__(self, server: "Server"):
        self.server = server
        self.text = ""
        self.filters: set = set()
        self.selected: Optional[DatasetEntry] = None

    @property
    def catalog(self) -> Catalog:
        return self.server.view

    def build(self):
        from nicegui import ui

        with ui.header().classes("items-center gap-4"):
            ui.label("datamaestro").classes("text-h6")
            ui.input(
                placeholder="Search (e.g. msmarco tag:english task:retrieval)",
                on_change=lambda e: self._search(e.value),
            ).props("dense dark standout clearable debounce=300").classes("grow")
            self.count = ui.label()
            ui.button(icon="refresh", on_click=self._refresh).props(
                "flat round color=white"
            ).tooltip("Reload the dataset definitions")

        with ui.left_drawer(bordered=True).classes("p-2"):
            self.facets = ui.column().classes("w-full gap-1")

        self.details_drawer = ui.right_drawer(value=False, bordered=True).props(
            "width=560"
        )
        with self.details_drawer:
            self.details = ui.column().classes("w-full p-2")

        self.table = ui.table(
            columns=COLUMNS, rows=[], row_key="id", pagination=50
        ).classes("w-full")
        self.table.props("dense flat")
        self.table.on("rowClick", lambda e: self._select(e.args[1]["id"]))

        self.errors = ui.column().classes("w-full")
        self._update()

    # --- Events

    def _search(self, text):
        self.text = text or ""
        self._update()

    def _toggle(self, name, value, selected):
        (self.filters.add if selected else self.filters.discard)((name, value))
        self._update()

    async def _refresh(self):
        from nicegui import run, ui

        ui.notify("Reloading the dataset definitions…")
        await run.io_bound(self.server.reload)
        self._update()
        if self.selected is not None:
            self._select(self.selected.id)
        ui.notify(f"{len(self.catalog)} datasets")

    def _select(self, dataset_id):
        self.selected = self.catalog.get(dataset_id)
        self.details.clear()
        if self.selected is not None:
            with self.details:
                Details(self.selected, self.server.downloads).build(self._close)
            self.details_drawer.show()

    def _close(self):
        self.selected = None
        self.details_drawer.hide()

    # --- Rendering

    def _update(self):
        from nicegui import ui

        downloads = self.server.downloads
        entries = self.catalog.search(make_query(self.text, self.filters))
        statuses = {value for key, value in self.filters if key == "status"}
        if statuses:
            entries = [e for e in entries if status(e, downloads) in statuses]
        self.table.rows = rows(entries, downloads)
        self.table.update()
        self.count.text = f"{len(entries)} / {len(self.catalog)} datasets"

        facets = self.catalog.facets(entries)
        facets["status"] = Counter(status(e, downloads) for e in entries)
        self.facets.clear()
        with self.facets:
            for name, title in FACETS:
                counts = facets[name]
                selected = {value for key, value in self.filters if key == name}
                if not counts and not selected:
                    continue
                ui.label(title).classes("text-subtitle2 mt-2")
                with ui.row().classes("gap-1"):
                    values = [v for v, _ in counts.most_common(MAX_FACET_VALUES)]
                    for value in sorted(selected | set(values)):
                        ui.chip(
                            f"{value} ({counts.get(value, 0)})",
                            selectable=True,
                            selected=value in selected,
                            color="primary" if value in selected else "grey-3",
                            on_selection_change=lambda e, n=name, v=value: self._toggle(
                                n, v, e.value
                            ),
                        ).props("dense")

        self.errors.clear()
        if self.catalog.errors:
            with self.errors:
                with ui.expansion(
                    f"{len(self.catalog.errors)} definition modules could not "
                    "be loaded",
                    icon="warning",
                ).classes("w-full text-warning"):
                    for error in self.catalog.errors:
                        with ui.expansion(f"{error.source}: {error.error}"):
                            ui.code(error.details, language="text")


class Details:
    """Details of a dataset"""

    def __init__(self, entry: DatasetEntry, downloads: Dict[str, "EntryMetadata"]):
        self.entry = entry
        self.download = downloads.get(entry.path) if entry.path else None
        self.values: Dict[str, Optional[str]] = {}
        self._dataset = None

    def build(self, close):
        from nicegui import ui

        entry = self.entry
        with ui.row().classes("w-full items-start no-wrap"):
            ui.label(entry.name or entry.id).classes("text-h6 grow")
            ui.button(icon="close", on_click=close).props("flat round dense")

        with ui.row().classes("items-center gap-1"):
            ui.label(entry.id).classes("font-mono")
            _copy_button(entry.id)
        if entry.aliases:
            ui.label(f"Aliases: {', '.join(entry.aliases)}").classes("text-caption")

        with ui.row().classes("gap-1"):
            for task in entry.tasks:
                ui.badge(task, color="secondary")
            for tag in entry.tags:
                ui.badge(tag, color="grey-6")

        with ui.column().classes("gap-0"):
            if entry.url:
                ui.link(entry.url, entry.url, new_tab=True)
            if entry.doi:
                doi = f"https://doi.org/{entry.doi}"
                ui.link(f"doi:{entry.doi}", doi, new_tab=True)
            if entry.config_type:
                ui.label(f"Type: {entry.config_type}").classes("text-caption")
            if entry.repository:
                version = (
                    f" (version {entry.repository_version})"
                    if entry.repository_version
                    else ""
                )
                ui.label(f"Repository: {entry.repository}{version}").classes(
                    "text-caption"
                )
            if entry.module:
                ui.label(f"Module: {entry.module}").classes("text-caption")
            if entry.requirements:
                ui.label(f"Requires: {', '.join(entry.requirements)}").classes(
                    "text-caption"
                )

        self._download_status()

        if not entry.available:
            ui.label(
                "This dataset is not defined in the current Python environment: "
                "it was last used with the repository version and module above. "
                "Install that version to use it, or delete its folder."
            ).classes("text-warning")
        elif entry.variants is not None:
            self._variants()
        else:
            self.usage = ui.column().classes("w-full")
            self._usage(entry.id)

        if entry.description:
            ui.separator()
            ui.markdown(entry.description)

    def _download_status(self):
        from nicegui import ui

        if self.download is None:
            ui.label("Not downloaded").classes("text-caption text-grey")
            return
        ui.label(
            f"Downloaded: {human_size(self.download.size)} "
            f"(created {_date(self.download.created)}, "
            f"last used {_date(self.download.last_access)})"
        ).classes("text-caption text-positive")
        if self.entry.variants is not None:
            ui.label("All the variants share this folder").classes(
                "text-caption text-grey"
            )

    def _variants(self):
        from nicegui import ui

        variants = self.entry.variants
        ui.separator()
        ui.label("Variants").classes("text-subtitle1")

        if variants.axes is None:
            # Not an axes-based variant space: shows some examples
            for example in variants.examples:
                ui.label(", ".join(f"{k}={v}" for k, v in example.items())).classes(
                    "font-mono text-caption"
                )
        else:
            with ui.column().classes("w-full gap-1"):
                for axis in variants.axes:
                    self._axis(axis)

        self.usage = ui.column().classes("w-full")
        self._update_variant()

        if variants.documentation:
            with ui.expansion("Documentation of the variants").classes("w-full"):
                _rst(variants.documentation)

    def _axis(self, axis):
        from nicegui import ui

        label = axis.name + ("" if axis.required else f" (default: {axis.default})")
        if axis.domain is not None:
            options = list(axis.domain)
            widget = ui.select(
                options,
                label=label,
                value=None,
                clearable=True,
                on_change=lambda e, a=axis.name: self._set(a, e.value),
            )
        else:
            widget = ui.input(
                label=label,
                placeholder=axis.type or "",
                on_change=lambda e, a=axis.name: self._set(a, e.value),
            ).props("debounce=300")
        widget.classes("w-full").props("dense")
        if axis.description:
            widget.tooltip(axis.description)

    def _set(self, axis: str, value):
        self.values[axis] = value
        self._update_variant()

    def _update_variant(self):
        from nicegui import ui

        if self.entry.variants.axes is None:
            self.usage.clear()
            with self.usage:
                ui.label("See the documentation of the variants to select one").classes(
                    "text-caption"
                )
            return

        if self._dataset is None:
            try:
                self._dataset = self.entry.dataset()
            except Exception as e:
                self.usage.clear()
                with self.usage:
                    ui.label(f"Could not load the dataset: {e}").classes(
                        "text-negative"
                    )
                return

        dataset_id, error = variant_id(self.entry, self.values, self._dataset)
        self._usage(dataset_id, error)

    def _usage(self, dataset_id: Optional[str], error: Optional[str] = None):
        from nicegui import ui

        self.usage.clear()
        with self.usage:
            if error:
                ui.label(error).classes("text-caption text-negative")
                return
            with ui.row().classes("items-center gap-1"):
                ui.label(dataset_id).classes("font-mono")
                _copy_button(dataset_id)
            ui.code(
                "from datamaestro import prepare_dataset\n\n"
                f'dataset = prepare_dataset("{dataset_id}")'
            ).classes("w-full")
            ui.code(f"datamaestro prepare '{dataset_id}'", language="bash").classes(
                "w-full"
            )


def _copy_button(text: str):
    from nicegui import ui

    ui.button(
        icon="content_copy",
        on_click=lambda: (ui.clipboard.write(text), ui.notify("Copied")),
    ).props("flat round dense size=sm")


def _rst(text: str):
    from nicegui import ui

    try:
        ui.restructured_text(text)
    except Exception:
        ui.code(text, language="rst")


class Server:
    """Shared state of the web server"""

    def __init__(self, context: "Context", *, refresh: bool = False):
        self.context = context
        self.catalog = Catalog.load(context, refresh=refresh)
        self._downloads: Optional[Dict[str, "EntryMetadata"]] = None
        self._view: Optional[Catalog] = None

    @property
    def downloads(self) -> Dict[str, "EntryMetadata"]:
        from datamaestro import cleaner

        if self._downloads is None:
            try:
                self._downloads = cleaner.entries(self.context)
            except Exception:
                self._downloads = {}
        return self._downloads

    @property
    def view(self) -> Catalog:
        """The catalog, with the downloaded datasets that are not defined in
        the current environment"""
        if self._view is None:
            self._view = self.catalog.with_downloads(self.downloads)
        return self._view

    def page(self):
        """Builds the page (for a new client)"""
        # Download status may have changed since the last page load
        self._downloads = self._view = None
        Browser(self).build()

    def reload(self):
        self.catalog = Catalog.load(self.context, refresh=True)
        self._downloads = self._view = None


def serve(
    context: "Context",
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
    refresh: bool = False,
    show: bool = False,
):
    """Starts the web server (blocks)"""
    try:
        from nicegui import ui
    except ImportError as e:
        raise ImportError(
            "The web interface needs NiceGUI: pip install datamaestro[gui]"
        ) from e

    server = Server(context, refresh=refresh)

    ui.run(
        server.page,
        host=host,
        port=port,
        title="datamaestro",
        reload=False,
        show=show,
        favicon="🗂",
    )
