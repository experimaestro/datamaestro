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
RE_DATAFILE = re.compile(r"^datasets/([^/]*)/(.*)\.md$")

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
    

    def on_config(self, config):
        self.repository_id = self.config['repository']
        nav = config["nav"]
        for datafile in self.repository.datafiles():
            path = "datasets/%s/%s.md" % (datafile.repository.id, datafile.id)
            nav.append({datafile.name: path})
        
        return config

    def on_files(self, files, config):
        for datafile in self.repository.datafiles():
            f = MkdocFile("datasets/%s/%s.md" % (datafile.repository.id, datafile.id), "", config["site_dir"], False)
            files.append(f)

        return files


    def on_page_read_source(self, item, config, page: MkdocPage, **kwargs):
        path = page.file.src_path

        m = RE_DATAFILE.match(path)
        if not m:
            return None

        df = self.repository.datafile(m.group(2))
        r = io.StringIO()
    
        r.write("# %s\n" % df.id)
        r.write(df.description)
        r.write("\n\n")
        for ds in df:
            if not ds.isalias:
                r.write("- %s [%s]\n" % (ds.get("name", ds.id), ds.get("handler", None)))

        return r.getvalue()

