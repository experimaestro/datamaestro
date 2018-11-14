import logging
import mkdocs
from mkdocs.commands import build, serve as mkserve
import mkdocs.config
import io
import yaml
import re
import mkdocs.plugins
from mkdocs.structure.files import File as MkdocFile
from ..context import Context

RE_DATAFILE = re.compile(r"^datasets/([^/]*)/(.*)\.md$")

class DatasetGenerator(mkdocs.plugins.BasePlugin):
    CONF = None
    config_scheme = (
        ('repository', mkdocs.config.config_options.Type(mkdocs.utils.text_type)),
    )

    @staticmethod
    def configuration() -> Context:
        if DatasetGenerator.CONF is None:
            DatasetGenerator.CONF = Context()
        return DatasetGenerator.CONF

    def on_pre_build(self, config):
        pass

    def on_config(self, config):
        self.repository_id = self.config['repository']

    def on_files(self, files, config):
        repository = DatasetGenerator.configuration().repository(self.repository_id)
        for datafile in repository.datafiles():
            files.append(MkdocFile("datasets/%s/%s.md" % (datafile.repository.id, datafile.id), "/datasets/", "", False))
            #{ datafile.id  :  })

        return files


    def on_page_read_source(self, _page, config, **kwargs):
        print(_page)
        return
        page = kwargs["page"]       
        path = page.input_path

        m = RE_DATAFILE.match(path)
        if not m:
            return None

        repository = DatasetGenerator.configuration().repository(m.group(1))
        df = repository.datafile(m.group(2))
        r = io.StringIO()
        r.write("# Description\n")
        r.write(df.description)
        r.write("\n\n")
        for ds in df:
            if not ds.isalias:
                r.write("- %s [%s]\n" % (ds.get("name", ds.id), ds.get("handler", None)))

        return r.getvalue()


def serve(config):
    DatasetGenerator.CONF = config
    mkserve.serve()

def generate(config):
    DatasetGenerator.CONF = config
    cfgfile = configfile(config)
    cfg = mkdocs.config.load_config() #cfgfile, pages=pages)
    build.build(cfg, dirty=False, live_server=True)
