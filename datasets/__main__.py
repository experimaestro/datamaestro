#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import argparse
import sys
import logging
import http.server
import os.path as op
import importlib
import yaml
from .xpm import ExperimaestroEncoder

from .data import Configuration
from .handlers.datasets import Dataset
from .commands import command, commands, arguments

logging.basicConfig(level=logging.INFO)

# --- Definition of commands

class HTTPHandler(http.server.SimpleHTTPRequestHandler):
    pass

def command_serve(args):
    import socketserver

    os.chdir(webpath(args))
    with socketserver.TCPServer(("", args.port), HTTPHandler) as httpd:
        print("serving at port", args.port)
        httpd.serve_forever()


# --- Manage repositories

@command(description="List repositories")
def repositories(config: Configuration, args):
    pass

@command(parent=repositories)
def list(config: Configuration, args):
    import pkgutil
    data = pkgutil.get_data('datasets', 'repositories.yaml')
    repositories = yaml.load(data)
    for key, info in repositories.items():
        print(key, info["description"])


# --- prepare and download

@arguments("dataset", help="The dataset ID")
@command()
def download(config: Configuration, args):
    dataset = Dataset.find(config, args.dataset)

    # Now, do something
    handler = dataset.getHandler()
    if not handler.download():
        logging.error("One or more errors occured while downloading the dataset")
        sys.exit(1)


@arguments("dataset", help="The dataset ID")
@command()
def prepare(config: Configuration, args):
    dataset = Dataset.find(config, args.dataset)

    handler = dataset.getHandler()
    handler.download()
    if not handler.download():
        logging.error("One or more errors occured while downloading the dataset")
        sys.exit(1)
    
    s = handler.prepare()
  
    try: 
        print(ExperimaestroEncoder().encode(s))
    except:
        logging.error("Error encoding to JSON: %s", s)
        sys.exit(1)


@arguments("dataset", help="The dataset ID")
@command()
def info(config: Configuration, args):
    dataset = Dataset.find(config, args.dataset)
    handler = dataset.getHandler()
    print(handler.description())


# --- Search

@arguments("regexp", help="The regular expression")
@command()
def search(config: Configuration, args):
    import re
    pattern = re.compile(args.regexp)
    for dataset in config.datasets():
        if any([pattern.search(id) is not None for id in dataset.ids]):
            print(dataset)

# --- Create the argument parser
def main():
    parser = argparse.ArgumentParser(description='datasets manager')
    parser.add_argument("--quiet", action="store_true", help="Be quiet")
    parser.add_argument("--debug", action="store_true", help="Be even more verbose (implies traceback)")
    parser.add_argument("--traceback", action="store_true", help="Display traceback if an exception occurs")
    parser.add_argument("--data", help="Directory containing datasets", default=Configuration.MAINDIR)

    parser.add_argument("command", choices=commands.keys())
    parser.add_argument("arguments", nargs=argparse.REMAINDER, help="Arguments for the preparation")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARN)
    elif args.debug:
        logging.getLogger().setLevel(logging.DEBUG)


    try:
        config = Configuration(args.data)
        commands[args.command](config, args)
    except Exception as e:
        sys.stderr.write("Error while running command %s:\n" % args.command)
        sys.stderr.write(str(e))

        if args.debug or args.traceback:
            import traceback
            sys.stderr.write(traceback.format_exc())

        sys.exit(1)
