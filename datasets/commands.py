import logging
import argparse

commands = {}

class Command:
    def __init__(self, method):
        self.method = method
        self.name = method.__name__ 
        self.parser = argparse.ArgumentParser(self.name)

    def __call__(self, config, args):
        logging.debug("Parsing remainding arguments: %s", args.arguments)
        self.parser.add_argument("arguments", nargs=argparse.REMAINDER, help="Arguments for the preparation")
        pargs = self.parser.parse_args(args.arguments)
        self.method(config, pargs)

class arguments:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
    
    def __call__(self, command):
        command.parser.add_argument(*self.args, **self.kwargs)
    

def command(m):
    c = Command(m)
    commands[c.name] = c
    return c
