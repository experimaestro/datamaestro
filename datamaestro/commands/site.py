import logging
import io
import yaml
import re
from pathlib import Path

import mkdocs
import mkdocs.config
import mkdocs.plugins
from mkdocs.structure.files import File as MkdocFile
from mkdocs.structure.pages import Page as MkdocPage
from mkdocs.structure.nav import Navigation as MkdocNavigation

from ..context import Context
from ..data import Repository

RE_DATAFILE = re.compile(r"^datamaestro/df/([^/]*)/(.*)\.md$")
RE_TAG = re.compile(r"^datamaestro/tag/([^/]*)\.md$")

class Matcher:
    def __init__(self):
        self.match = None

    def __call__(self, re, value):
        self.match = re.match(value)
        return self.match

    def group(self, *args):
        return self.match.group(*args)

MATCHER = Matcher()

class DatasetGenerator(mkdocs.plugins.BasePlugin):
    CONF = None
    REPOSITORY = None
    

    config_scheme = (
        ('repository', mkdocs.config.config_options.Type(mkdocs.utils.text_type)),
    )

    @staticmethod
    def configuration() -> Context:
        if DatasetGenerator.CONF is None:
            DatasetGenerator.CONF = Context()
        return DatasetGenerator.CONF

    @property
    def repository(self) -> Repository:
        if DatasetGenerator.REPOSITORY is None:
            DatasetGenerator.REPOSITORY = DatasetGenerator.configuration().repository(self.repository_id)
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
        self.repository_id = self.config['repository']
        self.datafiles = {}
        self.tags = {}
        nav = config["nav"]

        for datafile in self.repository.datafiles():
            self.datafiles[datafile.id] = datafile
            for dataset in datafile:
                for tag in dataset.tags():
                    self.tags.setdefault(tag, set()).add(dataset)
        
        for datafile in self.datafiles.values():
            path = "datamaestro/df/%s/%s.md" % (datafile.repository.id, datafile.id)
            nav.append({datafile.name: path})
        
        return config

    def on_files(self, files, config):
        for value in self.parse_nav(config["nav"]):
            if MATCHER(RE_TAG, value):
                f = MkdocFile("datamaestro/tag/%s.md" % (MATCHER.group(1)), "", config["site_dir"], False)
                files.append(f)
        for datafile in self.repository.datafiles():
            f = MkdocFile("datamaestro/df/%s/%s.md" % (datafile.repository.id, datafile.id), "", config["site_dir"], False)
            files.append(f)

        return files


    def on_page_read_source(self, item, config, page: MkdocPage, **kwargs):
        path = page.file.src_path

        if MATCHER(RE_TAG, path):
            r = io.StringIO()
            tag = MATCHER.group(1)
            r.write("# %s\n\n" %tag)
            for ds in self.tags.get(tag, set()):
                r.write("- [%s](/datamaestro/df/%s/%s.html)" % (ds.id, ds.datafile.repository.id, ds.datafile.id))

            return r.getvalue()

        m = RE_DATAFILE.match(path)
        if not m:
            return None

        df = self.repository.datafile(m.group(2))
        r = io.StringIO()
    
        r.write("# %s\n" % df.name)
        r.write(df.description)
        r.write("\n\n")
        for ds in df:
            if not ds.isalias:
                r.write("- %s [%s]\n" % (ds.get("name", ds.id), ds.get("handler", None)))

        return r.getvalue()

