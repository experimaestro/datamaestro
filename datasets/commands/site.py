import logging
import mkdocs
from mkdocs.commands import build, serve
import mkdocs.config
import io
import yaml
import mkdocs.plugins

class DatasetGenerator(mkdocs.plugins.BasePlugin):
    CONF = None

    def on_page_read_source(self, _page, config, **kwargs):
        page = kwargs["page"]
        print(page.input_path)
        return r"Placeholder"


def generate(config):
    DatasetGenerator.CONF = config
    pages = [{ 'Home': 'index.md' }]
    for repository in config.repositories():
        print(str(repository.name), pages)
        subpages = []
        pages.append({ str(repository.name) : subpages })
        for datafile in repository.datafiles():
            subpages.append({ datafile.id : 'a.md' })
        break
    configuration = {
        'site_name': 'Datasets',
        'pages': pages,
        'theme': 'readthedocs',
        'plugins': ['datasets']
    }

    
    configstring = yaml.dump(configuration)
    cfgfile = io.StringIO(configstring)
    # cfg = mkdocs.config.load_config(cfgfile, pages=pages)
    # build.build(cfg, dirty=False, live_server=True)
    serve.serve(cfgfile)
    # logging.warn("Sorry, not implemented yet")

    # mkdocs.commands.build()
