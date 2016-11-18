class Context(object):
    """Context of a configuration file"""
    def __init__(self, prefix):
        self.prefix = prefix
        
    def id(self, _id):
        return "%s.%s" % (self.prefix, _id)

class Data:
    def __init__(self, context, config):
        self.id = config.id
        if type(config.id) == list:
            self.aliases = self.id
            self.id = self.id[0]
        
class Documents:
    def __init__(self, context, config):
        pass