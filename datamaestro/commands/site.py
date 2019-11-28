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
from ..definitions import Repository

# Custom URIs for tags and datafiles
RE_DATAFILE = re.compile(r"^datamaestro/df/([^/]*)/(.*)\.md$")
RE_TASK = re.compile(r"^datamaestro/task/([^/]*)\.md$")

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
        files.append(MkdocFile("datamaestro/%s.md" % self.id, "", config["site_dir"], False))
        for key in self.map.keys():
            files.append(MkdocFile("datamaestro/%s/%s.md" % (self.id, key), "", config["site_dir"], False))

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
                r.write("- [%s](../df/%s/%s.html)\n" % (ds.get("name", ds.id), ds.datafile.repository.id, ds.datafile.id))

            return r.getvalue()
            
    @property
    def nav(self):
        nav = [{ "List of %s" % self.id : "datamaestro/%s.md" % self.id }]
        for key, item in self.map.items():
            nav.append({ item.name: "datamaestro/%s/%s.md" % (self.id, key) })
        return nav


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

        # Navigation
        nav = config["nav"]

        self.tags = Classification("Tags")
        self.tasks = Classification("Tasks")
        self.classifications = [self.tags, self.tasks]

        
        navdf = []
        nav.append({"Datasets": navdf})

        for datafile in self.repository.datafiles():
            path = "datamaestro/df/%s/%s.md" % (datafile.repository.id, datafile.id)
            navdf.append({datafile.name: path})
            self.datafiles[datafile.id] = datafile
            for dataset in datafile:
                for tag in dataset.tags:
                    self.tags.add(tag, dataset)
                for task in dataset.tasks:
                    self.tasks.add(task, dataset)
        
        for c in self.classifications:
            nav.append({c.name: c.nav})

        return config

    def on_files(self, files, config):
        files.append(MkdocFile("datamaestro/tasks.md", "", config["site_dir"], False))

        for c in self.classifications:
            c.addFiles(files, config)
        
        for datafile in self.repository.datafiles():
            # Add a file for each dataset
            f = MkdocFile("datamaestro/df/%s/%s.md" % (datafile.repository.id, datafile.id), "", config["site_dir"], False)
            files.append(f)

        return files


    def on_page_read_source(self, item, config, page: MkdocPage, **kwargs):
        """Generate pages"""
        path = page.file.src_path

        # --- Classifications
        for c in self.classifications:
            r = c.match(path)
            if r: 
                return r

        # --- Dataset file documentation generation

        m = RE_DATAFILE.match(path)
        if not m:
            return None

        df = self.repository.datafile(m.group(2))
        r = io.StringIO()
    
        r.write("# %s\n" % df.name)
        r.write(df.description)
        

        r.write("\n\n")
        if len(df) > 1: r.write("## List of datasets\n\n")
        for ds in df:
            if not ds.isalias:
                if len(df) > 1: r.write("### %s\n\n" % (ds.get("name", ds.id)))
                if ds.tags: r.write("**Tags**: %s \n" % ", ".join(ds.tags))
                if ds.tasks: r.write("**Tasks**: %s \n" % ", ".join(ds.tasks))

        return r.getvalue()

