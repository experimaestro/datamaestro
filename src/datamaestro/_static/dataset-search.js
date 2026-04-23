/* Datamaestro dataset search — MiniSearch-backed in-page widget.
 *
 * Loads `_static/datasets.json` (emitted by the `datamaestro.sphinx`
 * extension on build-finished), indexes the records, and renders
 * results + a details panel inside any `.dm-search` container present
 * on the page. */

(function () {
    "use strict";

    function findScriptSrc() {
        const scripts = document.getElementsByTagName("script");
        for (let i = 0; i < scripts.length; i++) {
            const src = scripts[i].src || "";
            if (src.indexOf("dataset-search.js") !== -1) return src;
        }
        return "";
    }

    function deriveStaticBase(scriptSrc) {
        // Strip the trailing `dataset-search.js[?query]` to get `…/_static/`.
        return scriptSrc.replace(/dataset-search\.js(\?.*)?$/, "");
    }

    function escapeHtml(str) {
        if (str === null || str === undefined) return "";
        return String(str)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    /* Best-effort reST inline → HTML for fields that come from
     * docstrings (name, description, variants doc). Handles the inline
     * markup we actually see in datamaestro datasets — interpreted-text
     * roles, double-backtick literals, single-backtick titles. Anything
     * fancier (cross-references resolution, lists, bold/italic) is left
     * as plain text since the docs page already renders the full reST. */
    function rstToHtml(str) {
        if (!str) return "";
        let out = escapeHtml(str);
        // :role:`~mod.Sym`, :role:`text <target>` → <code>cleaned</code>
        out = out.replace(/:[a-zA-Z]+:`([^`]+)`/g, function (_, body) {
            const cleaned = body.replace(/^~/, "").replace(/\s*&lt;[^&]+&gt;\s*$/, "");
            return "<code>" + cleaned + "</code>";
        });
        // ``literal``
        out = out.replace(/``([^`]+?)``/g, "<code>$1</code>");
        // `single-backtick title-reference`
        out = out.replace(/`([^`]+?)`/g, "<code>$1</code>");
        return out;
    }

    function truncate(text, max) {
        if (!text) return "";
        const t = String(text).trim();
        if (t.length <= max) return t;
        return t.slice(0, max).trimEnd() + "…";
    }

    function collectValues(records, key) {
        const set = new Set();
        records.forEach(function (rec) {
            (rec[key] || []).forEach(function (v) {
                if (v) set.add(v);
            });
        });
        return Array.from(set).sort(function (a, b) {
            return a.localeCompare(b);
        });
    }

    function highlightMatch(value, partial) {
        const escaped = escapeHtml(value);
        if (!partial) return escaped;
        const lower = value.toLowerCase();
        const idx = lower.indexOf(partial.toLowerCase());
        if (idx === -1) return escaped;
        // Reapply escaping segment-wise so the index survives.
        return escapeHtml(value.slice(0, idx)) +
            '<span class="dm-search-ac-match">' +
            escapeHtml(value.slice(idx, idx + partial.length)) +
            "</span>" +
            escapeHtml(value.slice(idx + partial.length));
    }

    function quoteIfNeeded(value) {
        return /\s/.test(value) ? '"' + value + '"' : value;
    }

    function init(container, records) {
        if (typeof MiniSearch === "undefined") {
            container.querySelector(".dm-search-stats").textContent =
                "Search library failed to load.";
            return;
        }

        const urlRoot = container.getAttribute("data-url-root") || "./";
        const input = container.querySelector(".dm-search-input");
        const stats = container.querySelector(".dm-search-stats");
        const list = container.querySelector(".dm-search-results");
        const details = container.querySelector(".dm-search-details");

        // Wrap the input so the autocomplete dropdown can be positioned
        // against it, and inject a facets row between stats and the
        // results layout.
        const inputWrap = document.createElement("div");
        inputWrap.className = "dm-search-input-wrap";
        input.parentNode.insertBefore(inputWrap, input);
        inputWrap.appendChild(input);
        const ac = document.createElement("ul");
        ac.className = "dm-search-autocomplete";
        ac.setAttribute("role", "listbox");
        ac.hidden = true;
        inputWrap.appendChild(ac);

        const facets = document.createElement("div");
        facets.className = "dm-search-facets";
        facets.hidden = true;
        const layout = container.querySelector(".dm-search-layout");
        container.insertBefore(facets, layout);

        // Index records by integer position so MiniSearch can store the
        // numeric id without overlapping dataset ids that contain dots.
        const indexed = records.map(function (rec, i) {
            return Object.assign({}, rec, {
                _idx: i,
                _tagsText: (rec.tags || []).join(" "),
                _tasksText: (rec.tasks || []).join(" "),
            });
        });

        // Universe of tag/task values for autocomplete and facet rendering.
        const TAG_LIST = collectValues(indexed, "tags");
        const TASK_LIST = collectValues(indexed, "tasks");
        const FACET_LIMIT = 15;

        const ms = new MiniSearch({
            idField: "_idx",
            fields: ["id", "name", "_tagsText", "_tasksText", "description"],
            storeFields: ["_idx"],
            searchOptions: {
                boost: { id: 3, name: 2.5, _tagsText: 2, _tasksText: 2 },
                prefix: true,
                fuzzy: 0.15,
                combineWith: "AND",
            },
        });
        ms.addAll(indexed);

        function renderListItem(rec) {
            const tags = (rec.tags || [])
                .map(function (t) { return '<span class="dm-tag">' + escapeHtml(t) + '</span>'; })
                .join("");
            const tasks = (rec.tasks || [])
                .map(function (t) { return '<span class="dm-task">' + escapeHtml(t) + '</span>'; })
                .join("");
            const meta = tags || tasks
                ? '<div class="dm-search-result-meta">' + tags + tasks + '</div>'
                : "";
            const name = rec.name
                ? '<div class="dm-search-result-name">' + rstToHtml(rec.name) + '</div>'
                : "";
            return (
                '<li class="dm-search-result" role="option" data-idx="' + rec._idx + '">' +
                '<div class="dm-search-result-id">' + escapeHtml(rec.id) + "</div>" +
                name + meta +
                "</li>"
            );
        }

        function buildDatasetUrl(rec) {
            if (!rec.docname) return null;
            return urlRoot + rec.docname + ".html#" + (rec.anchor || "");
        }

        function renderAxesTable(axes) {
            const keys = Object.keys(axes);
            if (!keys.length) return "";
            const rows = keys.map(function (k) {
                const a = axes[k];
                let domBody;
                if (a.domain && a.domain.length) {
                    // Pretty-printed JSON wraps better in narrow cells
                    // than the single-line form. The wrapper enables
                    // the per-cell scroll cap from the stylesheet.
                    domBody = '<div class="dm-search-axes-domain"><code>' +
                        escapeHtml(JSON.stringify(a.domain, null, 2)) +
                        "</code></div>";
                } else {
                    domBody = "<em>open</em>";
                }
                const def = a.has_default
                    ? '<code>' + escapeHtml(JSON.stringify(a.default)) + '</code>'
                    : '<em>required</em>';
                const flags = [];
                if (!a.in_id) flags.push("excluded from id");
                else if (a.elide_default) flags.push("elides default");
                const flagStr = flags.length
                    ? ' <small>(' + escapeHtml(flags.join("; ")) + ')</small>'
                    : "";
                return (
                    "<tr><td><code>" + escapeHtml(k) + "</code>" + flagStr + "</td>" +
                    "<td><code>" + escapeHtml(a.type || "") + "</code></td>" +
                    "<td>" + domBody + "</td>" +
                    "<td>" + def + "</td>" +
                    "<td>" + escapeHtml(a.description || "") + "</td></tr>"
                );
            }).join("");
            return (
                '<table class="dm-search-axes">' +
                "<colgroup>" +
                '<col style="width:18%">' +
                '<col style="width:12%">' +
                '<col style="width:28%">' +
                '<col style="width:14%">' +
                '<col style="width:28%">' +
                "</colgroup>" +
                "<thead><tr><th>axis</th><th>type</th><th>domain</th>" +
                "<th>default</th><th>description</th></tr></thead>" +
                "<tbody>" + rows + "</tbody></table>"
            );
        }

        function renderDetails(rec) {
            const parts = [];
            parts.push(
                '<button type="button" class="dm-search-back" ' +
                'aria-label="Back to results">' +
                '<span class="dm-search-back-icon" aria-hidden="true">←</span>' +
                ' Back to results</button>'
            );
            parts.push('<h3><code>' + escapeHtml(rec.id) + '</code></h3>');
            if (rec.name) {
                parts.push('<p class="dm-search-details-name">' + rstToHtml(rec.name) + '</p>');
            }
            if (rec.tags && rec.tags.length) {
                parts.push(
                    '<div class="dm-search-details-section">' +
                    '<div class="dm-search-details-label">Tags</div>' +
                    rec.tags.map(function (t) {
                        return '<span class="dm-tag">' + escapeHtml(t) + '</span>';
                    }).join(" ") + '</div>'
                );
            }
            if (rec.tasks && rec.tasks.length) {
                parts.push(
                    '<div class="dm-search-details-section">' +
                    '<div class="dm-search-details-label">Tasks</div>' +
                    rec.tasks.map(function (t) {
                        return '<span class="dm-task">' + escapeHtml(t) + '</span>';
                    }).join(" ") + '</div>'
                );
            }
            if (rec.configtype) {
                parts.push(
                    '<div class="dm-search-details-section">' +
                    '<div class="dm-search-details-label">Experimaestro type</div>' +
                    '<code>' + escapeHtml(rec.configtype) + '</code></div>'
                );
            }
            if (rec.url) {
                parts.push(
                    '<div class="dm-search-details-section">' +
                    '<div class="dm-search-details-label">External link</div>' +
                    '<a href="' + escapeHtml(rec.url) + '" target="_blank" rel="noopener">' +
                    escapeHtml(rec.url) + '</a></div>'
                );
            }
            if (rec.description) {
                parts.push(
                    '<div class="dm-search-details-section">' +
                    '<div class="dm-search-details-label">Description</div>' +
                    '<div class="dm-search-details-description">' +
                    rstToHtml(rec.description) + '</div></div>'
                );
            }
            if (rec.variants) {
                parts.push(
                    '<div class="dm-search-details-section">' +
                    '<div class="dm-search-details-label">Variants</div>' +
                    (rec.variants.description
                        ? '<div class="dm-search-details-description">' +
                          rstToHtml(rec.variants.description) + '</div>'
                        : "") +
                    (rec.variants.axes ? renderAxesTable(rec.variants.axes) : "") +
                    '</div>'
                );
            }
            const link = buildDatasetUrl(rec);
            if (link) {
                parts.push(
                    '<p class="dm-search-details-link">' +
                    '<a href="' + escapeHtml(link) + '">→ See full entry in the docs</a></p>'
                );
            }
            details.innerHTML = parts.join("");
        }

        function renderResults(hits) {
            list.innerHTML = hits.map(renderListItem).join("");
            renderFacets(hits);
        }

        function showInitialList() {
            // No query: show the first 50 records alphabetically by id
            // so the page is useful immediately.
            const initial = indexed.slice().sort(function (a, b) {
                return a.id.localeCompare(b.id);
            }).slice(0, 50);
            renderResults(initial);
            stats.textContent = indexed.length + " datasets indexed (showing first " +
                initial.length + ").";
        }

        // Maps a user-facing field name (the part before ``:``) to the
        // MiniSearch field actually indexed. Anything not in this map is
        // treated as free text.
        const FIELD_ALIASES = {
            tag: "_tagsText",
            tags: "_tagsText",
            task: "_tasksText",
            tasks: "_tasksText",
            id: "id",
            name: "name",
            description: "description",
            desc: "description",
        };

        /* Parse ``query`` into ``{ fielded: [...], free: "..." }`` where
         * each fielded entry is ``{ field, value }`` and ``free`` holds
         * any remaining unscoped tokens. Supports ``tag:foo`` and quoted
         * values like ``tag:"learning to rank"``. Unknown field prefixes
         * are kept in the free-text bucket so users still get matches if
         * they type something we don't recognize. */
        function parseQuery(query) {
            const TOKEN = /(\w+):"([^"]+)"|(\w+):(\S+)|"([^"]+)"|(\S+)/g;
            const fielded = [];
            const free = [];
            let m;
            while ((m = TOKEN.exec(query)) !== null) {
                const fieldKey = (m[1] || m[3] || "").toLowerCase();
                const fieldVal = m[2] || m[4];
                if (fieldKey && FIELD_ALIASES[fieldKey]) {
                    fielded.push({ field: FIELD_ALIASES[fieldKey], value: fieldVal });
                } else if (fieldKey) {
                    // Unknown prefix — treat the whole token as free text.
                    free.push(fieldKey + ":" + fieldVal);
                } else {
                    free.push(m[5] || m[6]);
                }
            }
            return { fielded: fielded, free: free.join(" ").trim() };
        }

        function searchHits(parsed) {
            if (!parsed.fielded.length && !parsed.free) return [];

            // Run each fielded clause as a separate single-field search,
            // intersect by id, and keep the most recent clause's ranking
            // for the final order. MiniSearch's per-call ``fields``
            // restricts the index columns considered, which is what
            // makes ``tag:foo`` only match the tag column.
            let resultIds = null;
            let lastOrdered = [];
            const intersect = function (ids) {
                if (resultIds === null) {
                    resultIds = new Set(ids);
                } else {
                    const next = new Set();
                    ids.forEach(function (i) { if (resultIds.has(i)) next.add(i); });
                    resultIds = next;
                }
                lastOrdered = ids;
            };

            for (let i = 0; i < parsed.fielded.length; i++) {
                const clause = parsed.fielded[i];
                const hits = ms.search(clause.value, {
                    fields: [clause.field],
                    prefix: true,
                    fuzzy: false,
                    combineWith: "AND",
                });
                intersect(hits.map(function (h) { return h._idx; }));
                if (resultIds.size === 0) return [];
            }

            if (parsed.free) {
                const hits = ms.search(parsed.free);
                intersect(hits.map(function (h) { return h._idx; }));
            }

            return lastOrdered
                .filter(function (i) { return resultIds.has(i); })
                .slice(0, 100)
                .map(function (i) { return indexed[i]; });
        }

        function search(query) {
            const q = query.trim();
            if (!q) {
                showInitialList();
                return;
            }
            const parsed = parseQuery(q);

            // Require 3 chars total of "useful" search input. Fielded
            // clauses contribute their value length; this lets ``tag:ir``
            // work even though the raw query is only 6 chars.
            const usefulLen = parsed.free.length +
                parsed.fielded.reduce(function (acc, c) { return acc + c.value.length; }, 0);
            if (usefulLen < 3) {
                renderResults([]);
                stats.textContent = "Type at least 3 characters to search (" +
                    indexed.length + " datasets indexed). " +
                    "Tip: use tag:foo or task:foo to filter by field.";
                return;
            }

            const hits = searchHits(parsed);
            renderResults(hits);
            stats.textContent = hits.length + " match" + (hits.length === 1 ? "" : "es") +
                " for \"" + query + "\".";
        }

        function showResults() {
            container.setAttribute("data-view", "results");
        }

        // ----- Autocomplete -----------------------------------------------

        // Matches a fielded clause whose value is being typed at the cursor.
        // Handles both unquoted (``tag:foo``) and open-quoted (``tag:"foo``)
        // forms. The match must end at the cursor (`$`).
        const FIELD_REGEX = /(?:^|\s)(tag|tags|task|tasks):(?:"([^"]*)$|([^\s"]*)$)/i;

        let acItems = [];
        let acIndex = -1;
        let acContext = null;  // { field, partial, partialStart, isQuoted }

        function fieldContext() {
            const cursor = input.selectionStart;
            const before = input.value.slice(0, cursor);
            const m = before.match(FIELD_REGEX);
            if (!m) return null;
            const field = m[1].toLowerCase();
            const isQuoted = m[2] !== undefined;
            const partial = isQuoted ? m[2] : (m[3] || "");
            const partialStart = before.length - partial.length;
            const list = (field === "tag" || field === "tags") ? TAG_LIST : TASK_LIST;
            return {
                field: field,
                partial: partial,
                partialStart: partialStart,
                isQuoted: isQuoted,
                source: list,
            };
        }

        function rankCandidates(source, partial) {
            const lower = partial.toLowerCase();
            return source
                .filter(function (v) {
                    return !lower || v.toLowerCase().indexOf(lower) !== -1;
                })
                .sort(function (a, b) {
                    if (!lower) return a.localeCompare(b);
                    const ap = a.toLowerCase().indexOf(lower);
                    const bp = b.toLowerCase().indexOf(lower);
                    if (ap !== bp) return ap - bp;  // prefix matches first
                    return a.localeCompare(b);
                })
                .slice(0, 12);
        }

        function showAutocomplete(ctx, items) {
            acContext = ctx;
            acItems = items;
            acIndex = items.length ? 0 : -1;
            const header = '<li class="dm-search-autocomplete-header" ' +
                'aria-hidden="true">' +
                (ctx.field.indexOf("tag") === 0 ? "Tags" : "Tasks") +
                ' matching "' + escapeHtml(ctx.partial) + '"</li>';
            if (!items.length) {
                ac.innerHTML = header +
                    '<li class="dm-search-autocomplete-empty">No matches.</li>';
            } else {
                ac.innerHTML = header + items.map(function (v, i) {
                    return '<li class="dm-search-ac-item' +
                        (i === acIndex ? " is-active" : "") +
                        '" role="option" data-ac-idx="' + i + '">' +
                        highlightMatch(v, ctx.partial) + "</li>";
                }).join("");
            }
            ac.hidden = false;
        }

        function hideAutocomplete() {
            ac.hidden = true;
            acContext = null;
            acItems = [];
            acIndex = -1;
        }

        function highlightAcItem(newIdx) {
            const items = ac.querySelectorAll(".dm-search-ac-item");
            if (!items.length) return;
            if (newIdx < 0) newIdx = items.length - 1;
            if (newIdx >= items.length) newIdx = 0;
            const previous = ac.querySelector(".dm-search-ac-item.is-active");
            if (previous) previous.classList.remove("is-active");
            items[newIdx].classList.add("is-active");
            items[newIdx].scrollIntoView({ block: "nearest" });
            acIndex = newIdx;
        }

        function applyAutocomplete(value) {
            if (!acContext) return;
            const cur = input.value;
            let after = cur.slice(input.selectionStart);
            const prefix = cur.slice(0, acContext.partialStart);
            let replacement;
            if (acContext.isQuoted) {
                if (after.charAt(0) === '"') after = after.slice(1);
                replacement = value + '"';
            } else if (/\s/.test(value)) {
                replacement = '"' + value + '"';
            } else {
                replacement = value;
            }
            const trailing = after.charAt(0) === " " || after === "" ? "" : " ";
            const newValue = prefix + replacement + trailing + after;
            input.value = newValue;
            const newCursor = (prefix + replacement + trailing).length;
            input.setSelectionRange(newCursor, newCursor);
            hideAutocomplete();
            search(newValue);
        }

        function refreshAutocomplete() {
            const ctx = fieldContext();
            if (!ctx) {
                hideAutocomplete();
                return;
            }
            showAutocomplete(ctx, rankCandidates(ctx.source, ctx.partial));
        }

        // ----- Facets -----------------------------------------------------

        function renderFacets(hits) {
            const tagsInResults = collectValues(hits, "tags");
            const tasksInResults = collectValues(hits, "tasks");

            // Empty input → fall back to globals (still bounded by FACET_LIMIT).
            const hasQuery = input.value.trim().length > 0;
            const tagSource = hasQuery ? tagsInResults : TAG_LIST;
            const taskSource = hasQuery ? tasksInResults : TASK_LIST;

            const showTags = tagSource.length > 0 && tagSource.length <= FACET_LIMIT;
            const showTasks = taskSource.length > 0 && taskSource.length <= FACET_LIMIT;

            if (!showTags && !showTasks) {
                facets.hidden = true;
                facets.innerHTML = "";
                return;
            }

            const groups = [];
            if (showTags) {
                groups.push(facetGroup("Tags", "tag", "", tagSource));
            }
            if (showTasks) {
                groups.push(facetGroup("Tasks", "task", "--task", taskSource));
            }
            facets.innerHTML = groups.join("");
            facets.hidden = false;
        }

        function facetGroup(label, field, modifier, values) {
            const buttons = values.map(function (v) {
                return '<button type="button" class="dm-search-facet-btn' +
                    (modifier ? " dm-search-facet-btn" + modifier : "") +
                    '" data-facet-field="' + field +
                    '" data-facet-value="' + escapeHtml(v) + '">' +
                    escapeHtml(v) + "</button>";
            }).join("");
            return '<div class="dm-search-facet-group">' +
                '<span class="dm-search-facet-label">' + label + "</span>" +
                buttons + "</div>";
        }

        function applyFacet(field, value) {
            const clause = field + ":" + quoteIfNeeded(value);
            const cur = input.value.trim();
            // Avoid duplicating an already-present clause.
            if (cur.indexOf(clause) !== -1) {
                input.focus();
                return;
            }
            input.value = (cur ? cur + " " : "") + clause + " ";
            input.focus();
            input.setSelectionRange(input.value.length, input.value.length);
            search(input.value);
        }

        facets.addEventListener("click", function (event) {
            const btn = event.target.closest(".dm-search-facet-btn");
            if (!btn) return;
            applyFacet(
                btn.getAttribute("data-facet-field"),
                btn.getAttribute("data-facet-value")
            );
        });

        ac.addEventListener("mousedown", function (event) {
            // Use mousedown so the input doesn't lose focus before we apply.
            const li = event.target.closest(".dm-search-ac-item");
            if (!li) return;
            event.preventDefault();
            const i = parseInt(li.getAttribute("data-ac-idx"), 10);
            if (!isNaN(i) && acItems[i]) applyAutocomplete(acItems[i]);
        });

        ac.addEventListener("mouseover", function (event) {
            const li = event.target.closest(".dm-search-ac-item");
            if (!li) return;
            const i = parseInt(li.getAttribute("data-ac-idx"), 10);
            if (!isNaN(i)) highlightAcItem(i);
        });

        input.addEventListener("blur", function () {
            // Delay so a click on the dropdown still registers.
            setTimeout(hideAutocomplete, 100);
        });

        function showDetailsFor(idx) {
            const previous = list.querySelector(".dm-search-result.is-active");
            if (previous) previous.classList.remove("is-active");
            const li = list.querySelector('.dm-search-result[data-idx="' + idx + '"]');
            if (li) li.classList.add("is-active");
            renderDetails(indexed[idx]);
            container.setAttribute("data-view", "details");
            // Bring the panel into view (helpful on long pages).
            details.scrollIntoView({ block: "nearest", behavior: "smooth" });
        }

        list.addEventListener("click", function (event) {
            const li = event.target.closest(".dm-search-result");
            if (!li) return;
            const idx = parseInt(li.getAttribute("data-idx"), 10);
            if (isNaN(idx)) return;
            showDetailsFor(idx);
        });

        details.addEventListener("click", function (event) {
            const back = event.target.closest(".dm-search-back");
            if (!back) return;
            event.preventDefault();
            showResults();
            input.focus();
        });

        let pending = null;
        input.addEventListener("input", function () {
            if (pending) clearTimeout(pending);
            const value = input.value;
            // Typing implicitly returns to the results view.
            showResults();
            refreshAutocomplete();
            pending = setTimeout(function () { search(value); }, 80);
        });

        // Cursor moves (arrow keys without changing text) should also
        // refresh the dropdown so it reflects the new context.
        input.addEventListener("keyup", function (event) {
            if (event.key === "ArrowLeft" || event.key === "ArrowRight" ||
                event.key === "Home" || event.key === "End") {
                refreshAutocomplete();
            }
        });

        input.addEventListener("focus", refreshAutocomplete);
        input.addEventListener("click", refreshAutocomplete);

        // Keyboard navigation: when the autocomplete is open, ↓↑/Enter/Tab
        // drive it; Esc closes it. Outside the dropdown, Esc switches the
        // widget back to the results view.
        input.addEventListener("keydown", function (event) {
            if (!ac.hidden && acItems.length) {
                if (event.key === "ArrowDown") {
                    event.preventDefault();
                    highlightAcItem(acIndex + 1);
                    return;
                }
                if (event.key === "ArrowUp") {
                    event.preventDefault();
                    highlightAcItem(acIndex - 1);
                    return;
                }
                if (event.key === "Enter" || event.key === "Tab") {
                    if (acIndex >= 0 && acItems[acIndex]) {
                        event.preventDefault();
                        applyAutocomplete(acItems[acIndex]);
                        return;
                    }
                }
                if (event.key === "Escape") {
                    event.preventDefault();
                    hideAutocomplete();
                    return;
                }
            }
            if (event.key === "Escape" &&
                container.getAttribute("data-view") === "details") {
                showResults();
                input.focus();
            }
        });

        showResults();
        showInitialList();
    }

    function reportError(containers, message) {
        containers.forEach(function (c) {
            const stats = c.querySelector(".dm-search-stats");
            if (stats) stats.textContent = message;
        });
    }

    function loadViaScriptTag(staticBase) {
        // Inject `datasets.js` (which assigns to window.__DATAMAESTRO_DATASETS__).
        // `<script src>` is CORS-exempt so this works under file:// too,
        // unlike fetch().
        return new Promise(function (resolve, reject) {
            const tag = document.createElement("script");
            tag.src = staticBase + "datasets.js";
            tag.async = true;
            tag.onload = function () {
                if (window.__DATAMAESTRO_DATASETS__) {
                    resolve(window.__DATAMAESTRO_DATASETS__);
                } else {
                    reject(new Error("datasets.js loaded but no data global set"));
                }
            };
            tag.onerror = function () {
                reject(new Error("could not load " + tag.src));
            };
            document.head.appendChild(tag);
        });
    }

    function loadAndInit() {
        const containers = document.querySelectorAll(".dm-search");
        if (!containers.length) return;

        const scriptSrc = findScriptSrc();
        const staticBase = deriveStaticBase(scriptSrc);

        const ready = window.__DATAMAESTRO_DATASETS__
            ? Promise.resolve(window.__DATAMAESTRO_DATASETS__)
            : loadViaScriptTag(staticBase).catch(function (scriptErr) {
                // Last-ditch fallback for hosts that serve datasets.json
                // but not datasets.js.
                return fetch(staticBase + "datasets.json").then(function (r) {
                    if (!r.ok) throw new Error("HTTP " + r.status);
                    return r.json();
                }).catch(function (fetchErr) {
                    throw new Error(scriptErr.message + "; " + fetchErr.message);
                });
            });

        ready
            .then(function (records) {
                containers.forEach(function (c) { init(c, records); });
            })
            .catch(function (err) {
                reportError(containers, "Failed to load dataset index: " + err.message);
            });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", loadAndInit);
    } else {
        loadAndInit();
    }
})();
