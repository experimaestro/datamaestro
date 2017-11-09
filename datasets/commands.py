import logging
import argparse
from functools import wraps

commands = {}

class Command:
    def __init__(self, method, description=None, allowextraargs=False):
        self.method = method

        self.description = description
        self.name = method.__name__.replace('_', '-')
        self.parser = argparse.ArgumentParser(self.name)
        self.subcommands = {}
        self.allowextraargs = allowextraargs

    def __call__(self, config, args):
        logging.debug("Parsing remaining arguments: %s", args.arguments)

        if self.subcommands:
            subparsers = self.parser.add_subparsers()
            for key, command in self.subcommands.items():
                parser = subparsers.add_parser(key, help=command.description)
                parser.set_defaults(subcommand=command)
                parser.add_argument("arguments", nargs=argparse.REMAINDER, 
                    help="Arguments for %s" % key)
        elif self.allowextraargs:
            self.parser.add_argument("arguments", nargs=argparse.REMAINDER, 
                help="Arguments for the preparation")
        
        pargs = self.parser.parse_args(args.arguments)
        
        self.method(config, pargs)
        if self.subcommands and pargs.subcommand:
            pargs.subcommand(config, pargs)


class arguments:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
    
    def __call__(self, command):
        command.parser.add_argument(*self.args, **self.kwargs)
    

class command:
    def __init__(self, description=None, parent=None):
        self.description = description
        self.parent = parent

    def __call__(self, m):    
        c = Command(m, description=self.description)
        if self.parent:
            self.parent.subcommands[c.name] = c
        else:
            commands[c.name] = c
        return c
