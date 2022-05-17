# Sphinx extension for datamaestro datasets

import logging
from typing import Any, Dict, List, Optional, Tuple
import importlib
from sphinx.ext.autodoc.mock import mock

from docutils import nodes

from sphinx.application import Sphinx
from sphinx.domains import Domain, ObjType, XRefRole
from sphinx.util.docutils import SphinxDirective
from sphinx.directives import ObjectDescription
from sphinx.locale import _, __
from sphinx import addnodes
from sphinx.util.nodes import make_refnode
from myst_parser.main import to_docutils
import datamaestro
from datamaestro.context import Datasets
from datamaestro.data import AbstractDataset


class DatasetNode(nodes.paragraph):
    pass


class DatasetsDirective(SphinxDirective):
    def dataset_desc(self, ds: AbstractDataset):

        dm = self.env.get_domain("dm")
        dm.add_dataset(ds.id)

        # indexnode = addnodes.index(entries=[])
        desc = addnodes.desc()
        desc["domain"] = DatamaestroDomain.name
        desc["objtype"] = desc["desctype"] = "dataset"
        desc["classes"].append(DatamaestroDomain.name)

        signodes = addnodes.desc_signature(ds.id, "", is_multiline=True)
        desc.append(signodes)

        signode = addnodes.desc_signature_line()
        signode += nodes.Text("Dataset ")
        signode += addnodes.desc_name(text=ds.id)
        signode["ids"].append("dataset" + "-" + ds.id)
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
            content.extend(to_docutils(ds.description))

        return desc


class RepositoryDirective(DatasetsDirective):
    """Generates the document for a whole repository"""

    has_content = True
    required_arguments = 1
    optional_arguments = 0

    def run(self):
        (repository_id,) = self.arguments
        with mock(self.config.autodoc_mock_imports):
            repository = datamaestro.Context.instance().repository(
                repository_id
            )  # type: Optional[datamaestro.Repository]
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
                    section += to_docutils(module.description).children

                for ds in module.datasets:
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

        docnodes = []
        # node.document = self.state.document
        if datasets.description:
            docnodes.extend(to_docutils(datasets.description))

        for ds in datasets:
            docnodes.append(self.dataset_desc(ds))
        return docnodes


class DatamaestroDomain(Domain):
    name = "dm"
    object_types = {
        "dataset": ObjType(_("dataset"), "ds"),
    }
    directives = {
        "repository": RepositoryDirective,
        "datasets": DatasetDirective,
    }
    roles = {"ref": XRefRole()}
    indices = {
        # TODO: Add indices for tags and tasks
    }
    initial_data: Dict[str, Dict[str, Tuple[str, str]]] = {
        "datasets": {},  # fullname -> dataset
        "tags": {},  # tag  -> list of datasets,
        "tasks": {},  # task name -> list of datasets
    }

    def add_dataset(self, dsid):
        self.data["datasets"][dsid] = (self.env.docname, f"dataset-{dsid}")

    def resolve_xref(self, env, fromdocname, builder, typ, target, node, contnode):
        print("[dm/sphinx] Searching for", target)

        ref = self.data["datasets"].get(target, None)
        if ref:
            docname, targ = ref
            return make_refnode(builder, fromdocname, docname, targ, contnode, targ)
        return None


def setup(app: Sphinx) -> Dict[str, Any]:
    """Setup experimaestro for Sphinx documentation"""

    app.add_domain(DatamaestroDomain)
    app.add_node(DatasetNode)

    app.add_config_value("datamaestro_repository", None, True)

    return {"version": datamaestro.__version__, "parallel_read_safe": True}
