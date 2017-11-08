#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import argparse
import sys
import logging
import http.server
import os.path as op
import importlib
import yaml

from .data import Dataset, DataFile, Configuration
from .commands import command, commands, arguments

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
def repositories():
    pass

@command(parent=repositories)
def list():
    import pkgutil
    data = pkgutil.get_data('datasets', 'repositories.yaml')
    repositories = yaml.load(data)
    for key, info in repositories.items():
        print(key, info["description"])



# --- prepare and download

@arguments("dataset", help="The dataset ID")
@command()
def prepare(args, pargs):
    dataset = Dataset.find(pargs.dataset)
    # Now, do something
    handler = dataset.getHandler()
    handler.download()
    handler.prepare()

@arguments("dataset", help="The dataset ID")
@command()
def download():
    dataset = Dataset.find(config, dataset)

    # Now, do something
    handler = dataset.getHandler(config)
    handler.download()

@arguments("dataset", help="The dataset ID")
@command()
def info():
    dataset = Dataset.find(config, dataset)
    handler = dataset.getHandler(config)
    print(handler.description())


# --- Search

@command()
def search(config: Configuration, args):
    for df in config.files():
        print(df)
    # for root, dirs, files in os.walk(cpath, topdown=False):
    #     for relpath in files:
    #         if relpath.endswith(YAML_SUFFIX):
    #             path = op.join(root, relpath)
    #             prefix = op.relpath(path, cpath)[:-len(YAML_SUFFIX)].replace("/", ".")
    #             data = readyaml(path)
    #             if data is not None and "data" in data:
    #                 for d in data["data"]:
    #                     if type(d["id"]) == list:
    #                         for _id in d["id"]:
    #                             print("%s.%s" % (prefix, _id))
    #                     else:
    #                         print("%s.%s" % (prefix, d["id"]))


# --- Create the argument parser
def main():
    parser = argparse.ArgumentParser(description='datasets manager')
    parser.add_argument("--verbose", action="store_true", help="Be verbose")
    parser.add_argument("--debug", action="store_true", help="Be even more verbose (implies traceback)")
    parser.add_argument("--traceback", action="store_true", help="Display traceback if an exception occurs")
    parser.add_argument("--data", help="Directory containing datasets", default=Configuration.MAINDIR)

    parser.add_argument("command", choices=commands.keys())
    parser.add_argument("arguments", nargs=argparse.REMAINDER, help="Arguments for the preparation")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit()

    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    if args.debug:
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
