import logging
import mkdocs
from mkdocs.commands import build, serve as mkserve
import mkdocs.config
import io
import yaml
import re
import mkdocs.plugins

RE_DATAFILE = re.compile(r"^datasets/([^/]*)/(.*)\.md$")

class DatasetGenerator(mkdocs.plugins.BasePlugin):
    CONF = None
    def on_page_read_source(self, _page, config, **kwargs):
        page = kwargs["page"]       
        path = page.input_path

        m = RE_DATAFILE.match(path)
        if m:
            repository = DatasetGenerator.CONF.repository(m.group(1))
            df = repository.datafile(m.group(2))
            r = io.StringIO()
            r.write("# Description\n")
            r.write(df.description)
            r.write("\n\n")
            for ds in df:
                if not ds.isalias:
                    r.write("- %s [%s]\n" % (ds.get("name", ds.id), ds.get("handler", None)))

            return r.getvalue()
        return "Unhandled path : %s" % (page.input_path)

def configfile(config):
    DatasetGenerator.CONF = config
    pages = [{ 'Home': 'index.md' }]
    for repository in config.repositories():
        print(str(repository.name), pages)
        subpages = []
        pages.append({ str(repository.name) : subpages })
        for datafile in repository.datafiles():
            subpages.append({ datafile.id  : "datasets/%s/%s.md" % (datafile.repository.id, datafile.id) })
    
    configuration = {
        'site_name': 'Datasets',
        'pages': pages,
        'theme': 'readthedocs',
        'plugins': ['datasets']
    }
    configstring = yaml.dump(configuration)
    cfgfile = io.StringIO(configstring)
    return cfgfile
    
def serve(config):
    cfgfile = configfile(config)
    mkserve.serve(cfgfile)


def generate(config):
    cfgfile = configfile(config)
    cfg = mkdocs.config.load_config(cfgfile, pages=pages)
    build.build(cfg, dirty=False, live_server=True)
