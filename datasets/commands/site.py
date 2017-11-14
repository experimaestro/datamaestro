import logging
import mkdocs
# import mkdocs.plugins

# class DatasetGenerator(mkdocs.plugins.BasePlugin):
#     pass

def generate():
    entry_points= {
        'mkdocs.plugins': [
            'pluginname = path.to.some_plugin:SomePluginClass',
        ]
    }
    mkdocs.commands.build()
    logging.warn("Sorry, not implemented yet")
    for dataset in config.datasets():
        print(dataset)
