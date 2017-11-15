from . import data

class Handler(data.Documents):
    def __init__(self, context, config):
        data.Documents.__init__(self, context, config)
