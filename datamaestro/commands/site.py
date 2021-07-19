from inspect import ismodule
import logging
import io
import re
from pathlib import Path
import inspect
import typing
from typing import Dict, Iterable, Optional
import importlib
from datamaestro.definitions import AbstractDataset

import mkdocs
import mkdocs.config
import mkdocs.plugins
from mkdocs.structure.files import File as MkdocFile
from mkdocs.structure.pages import Page as MkdocPage

from docstring_parser import parse as docstring_parse

import experimaestro
from experimaestro.core.types import ObjectType

from ..context import Context, Repository, Datasets


# Custom URIs for tags and modules
RE_MODULE = re.compile(r"^datamaestro/df/([^/]*)/(.*)\.md$")
RE_TASK = re.compile(r"^datamaestro/task/([^/]*)\.md$")
RE_APIGEN = re.compile(r"@@api:(.+)")


class Matcher:
    def __init__(self):
        self.match = None

    def __call__(self, re, value):
        self.match = re.match(value)
        return self.match

    def group(self, *args):
        return self.match.group(*args)


MATCHER = Matcher()


class ClassificationItem:
    def __init__(self, name):
        self.values = []
        self.name = name


def method_documentation(doc, types):
    s = ""
    for param in doc.params:
        type_name = param.type_name
        if not type_name and param.arg_name in types:
            t = types[param.arg_name]
            if isinstance(t, typing._Final):
                type_name = str(t).replace("typing.", "")
            else:
                type_name = types[param.arg_name].__name__
        s += " - `{}` (`{}`): {}\n".format(
            param.arg_name, type_name or "?", param.description or ""
        )
    return s


def document_data(datatype: ObjectType):
    xpm = datatype.__getxpmtype__()
    s = "### %s\n\nClass `%s.%s`\n\n" % (
        xpm.identifier,
        datatype.__module__,
        datatype.__name__,
    )

    supertypes = ", ".join(str(parent.identifier) for parent in xpm.parents())
    if supertypes:
        s += "**Supertypes**: %s\n\n" % supertypes

    arguments = list(xpm.arguments.items())
    if arguments:
        s += "\n**Arguments**\n\n"
        for name, argument in arguments:
            s += "- **%s**: %s\n" % (name, argument.help)

    for name, method in inspect.getmembers(datatype):
        if inspect.isfunction(method) and hasattr(method, "__datamaestro_doc__"):
            try:
                doc = docstring_parse(method.__datamaestro_doc__)

                signature = re.sub(r"\(self,?", "(", str(inspect.signature(method)))
                s += "#### %s%s\n" % (name, signature)

                if doc.short_description:
                    s += doc.short_description + "\n"
                if doc.long_description:
                    s += doc.long_description + "\n"
                s += method_documentation(doc, method.__annotations__)
            except Exception as e:
                logging.error(
                    "Error while parsing documetnation of %s (%s)",
                    method,
                    method.__module__,
                )

    return s


def document_object(object):
    from datamaestro.data import Base

    try:
        name = object.__name__
        # Get the documentation
        if inspect.isclass(object):
            if hasattr(object, "__datamaestro__"):
                return document_data(object)

            docstring = object.__init__.__doc__
            types = object.__init__.__annotations__
            signature = str(inspect.signature(object.__init__)).replace("(self, ", "(")

        else:
            docstring = object.__doc__
            types = object.__annotations__
            signature = str(inspect.signature(object))

        doc = docstring_parse(docstring)
        if doc.short_description:
            s = "### " + doc.short_description + "\n\n"
        else:
            s = "### %s\n\n" % name

        s += "`@{}{}`\n\n".format(name, signature)

        if doc.long_description:
            s += doc.long_description

        s += method_documentation(doc, types)

        return s

    except Exception as e:
        logging.exception(
            "Exception while generating the documentation for %s" % object.__name__
        )
        return r"""<div class="error">Documentation error for {}</div>""".format(
            object.__name__
        )


def document(match):
    """Generate the documentation"""
    from datamaestro.data import Base

    modulename, name = match.group(1).rsplit(".", 1)

    module = importlib.import_module(modulename)
    try:
        object = getattr(module, name)
    except:
        return "<div class='error'>Cannot find %s in %s</div>" % (name, modulename)

    if ismodule(object):
        return "\n\n".join(
            document_data(o)
            for o in object.__dir__()
            if inspect.isclass(o) and hasattr(Base, "__datamaestro__")
        )
    return document_object(object)


class Classification:
    def __init__(self, name):
        self.id = name.lower()
        self.name = name
        # Maps keys to couple
        self.map = {}

        self.re = re.compile(r"^datamaestro/%s/([^/]*)\.md$" % (self.id))

    def add(self, name, value):
        key = name.lower()
        if not key in self.map:
            self.map[key] = ClassificationItem(name)
        self.map[key].values.append(value)

    def addFiles(self, files, config):
        files.append(
            MkdocFile("datamaestro/%s.md" % self.id, "", config["site_dir"], False)
        )
        for key in self.map.keys():
            files.append(
                MkdocFile(
                    "datamaestro/%s/%s.md" % (self.id, key),
                    "",
                    config["site_dir"],
                    False,
                )
            )

    def match(self, path):

        if path == "datamaestro/%s.md" % self.id:
            r = io.StringIO()
            r.write("# List of %s\n\n" % self.name)
            for key, value in sorted(self.map.items(), key=lambda kv: kv[0]):
                r.write("- [%s](%s/%s.html)\n" % (value.name, self.id, key))
            return r.getvalue()

        if MATCHER(self.re, path):
            # Case of a tag
            r = io.StringIO()
            key = MATCHER.group(1)
            item = self.map[key]
            r.write("# %s\n\n" % item.name)

            for ds in item.values:
                meta = ds

                module = Datasets(importlib.import_module(meta.t.__module__))
                r.write(
                    "- [%s](../df/%s/%s.html#%s)\n"
                    % (meta.name or meta.id, meta.repository.id, module.id, meta.id)
                )

            return r.getvalue()

    @property
    def nav(self):
        nav = [{"List of %s" % self.id: "datamaestro/%s.md" % self.id}]
        for key, item in self.map.items():
            nav.append({item.name: "datamaestro/%s/%s.md" % (self.id, key)})
        return nav


class DatasetGenerator(mkdocs.plugins.BasePlugin):
    """Mkdocs plugin for datamaestro submodules

    See:
        https://www.mkdocs.org/user-guide/plugins/ for mkdocs plugins

    Arguments:
        mkdocs {[type]} -- [description]
    """

    CONF: Optional[Context] = None
    REPOSITORY: Optional[Repository] = None

    config_scheme = (("repository", mkdocs.config.config_options.Type(str)),)
    experimaestro_plugin = experimaestro.mkdocs.base.Documentation

    def on_pre_build(self, config):
        self.experimaestro_plugin = config["plugins"].get("experimaestro")
        if self.experimaestro_plugin is None:
            logging.warning(
                "No experimaestro plugin found: won't generate type information"
            )

    @staticmethod
    def configuration() -> Context:
        if DatasetGenerator.CONF is None:
            DatasetGenerator.CONF = Context()
        return DatasetGenerator.CONF

    @property
    def repository(self) -> Repository:
        if DatasetGenerator.REPOSITORY is None:
            DatasetGenerator.REPOSITORY = DatasetGenerator.configuration().repository(
                self.repository_id
            )
        return DatasetGenerator.REPOSITORY

    def parse_nav(self, nav):
        for entry in nav:
            assert len(entry) == 1
            key, value = *entry.keys(), *entry.values()
            if isinstance(value, list):
                for value in self.parse_nav(value):
                    yield value
            else:
                yield value

    def on_config(self, config):
        self.repository_id = self.config["repository"]
        self.classifications = []
        self.modules: Dict[str, Iterable[AbstractDataset]] = {}

        if not self.repository_id:
            return

        config["extra_css"].insert(0, "mainstyle.css")
        # Navigation
        nav = config["nav"]

        self.tags = Classification("Tags")
        self.tasks = Classification("Tasks")
        self.classifications = [self.tags, self.tasks]

        navdf = []
        nav.append({"Datasets": navdf})

        for module in self.repository.modules():
            if not module.id:
                continue
            path = "datamaestro/df/%s/%s.md" % (self.repository_id, module.id)
            hasdataset = False
            for dataset in module:
                hasdataset = True
                for tag in dataset.tags:
                    self.tags.add(tag, dataset)
                for task in dataset.tasks:
                    self.tasks.add(task, dataset)

            if hasdataset:
                self.modules[module.id] = module
                navdf.append({module.id: path})

        for c in self.classifications:
            nav.append({c.name: c.nav})

        return config

    def on_post_build(self, config):
        """Called after the build"""
        import importlib.resources
        import shutil

        path = Path(config["site_dir"]) / "mainstyle.css"
        with importlib.resources.open_binary(
            "datamaestro.commands", "mainstyle.css"
        ) as source, path.open("wb") as dest:
            shutil.copyfileobj(source, dest)

    def on_files(self, files, config):
        """Called when list of files has been read"""
        if self.repository_id:
            files.append(
                MkdocFile("datamaestro/tasks.md", "", config["site_dir"], False)
            )
            for c in self.classifications:
                c.addFiles(files, config)

            for module in self.modules.values():
                # Add a file for each dataset
                f = MkdocFile(
                    "datamaestro/df/%s/%s.md" % (self.repository_id, module.id),
                    "",
                    config["site_dir"],
                    False,
                )
                files.append(f)
        return files

    def on_serve(self, server, config, builder):
        """Refresh when changing source code"""
        import datamaestro

        path = (
            self.repository.basedir
            if self.repository
            else str(Path(datamaestro.__file__).parent)
        )
        basemodule = (
            "%s." % self.repository.module if self.repository else "datamaestro."
        )

        def rebuild():
            import sys

            # Clear-up loaded module
            toremove = [
                module for module in sys.modules if module.startswith(basemodule)
            ]
            for module in toremove:
                del sys.modules[module]

            # Remove defined data
            ObjectType.REGISTERED = {}

            builder()

        logging.info("Watching %s", path)
        server.watch(path, rebuild)

    def on_page_markdown(self, markdown, page, config, **kwargs):
        if page.url.startswith("api/"):
            return RE_APIGEN.sub(document, markdown)
        if self.repository and page.url == "":
            return (
                "Documentation for datamaestro module **%s (version %s)**\n\n"
                % (self.repository.NAMESPACE, self.repository.version())
            ) + markdown
        return markdown

    def on_page_read_source(self, page: MkdocPage, **kwargs):
        """Generate pages"""
        path = page.file.src_path

        # --- Classifications
        for c in self.classifications:
            r = c.match(path)
            if r:
                return r

        # --- Dataset file documentation generation

        m = RE_MODULE.match(path)
        if not m:
            return None

        df = self.modules[m.group(2)]
        r = io.StringIO()

        r.write("# %s\n" % df.id)
        r.write(df.description)

        r.write("\n\n")
        r.write("## List of datasets\n\n")
        for ds in df:
            r.write(
                """<div class="dataset-entry"><div class='dataset-id'>%s<a name="%s"></a></div>\n\n"""
                % (ds.id, ds.id)
            )
            if ds.name:
                r.write("<div class='dataset-name'>%s</div>\n\n" % ds.name)

            if ds.tags or ds.tasks:
                r.write("<div class='tagtask'>")
                if ds.tags:
                    r.write(
                        "".join("<span class='tag'>%s</span>" % tag for tag in ds.tags)
                    )
                if ds.tasks:
                    r.write(
                        "".join(
                            "<span class='task'>%s</span>" % task for task in ds.tasks
                        )
                    )
                r.write("</div>")

            if ds.url:
                r.write("""<div><a href="{0}">{0}</a></div>""".format(ds.url))
            if ds.description:
                r.write("<div class='description'>%s</div>" % ds.description)

            if self.experimaestro_plugin and ds.configtype:
                qualname, href = self.experimaestro_plugin.getConfigLink(
                    page.url, ds.configtype
                )
                if href:
                    r.write(
                        f"""<div><b>Type</b>: <a href="{href}">{qualname}</a></div>"""
                    )
                else:
                    r.write(f"<div><b>Type</b>: {qualname}</div>")

            r.write("</div>")

        return r.getvalue()
