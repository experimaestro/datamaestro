#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import argparse
import sys
import logging
import http.server
import os.path as op
import importlib

from .data import Dataset, DataFile, Configuration
from .commands import command, commands, arguments

MAINDIR = op.join(op.dirname(op.dirname(__file__)), "share")

# --- Definition of commands

class HTTPHandler(http.server.SimpleHTTPRequestHandler):
    pass

def command_serve(args):
    import socketserver

    os.chdir(webpath(args))
    with socketserver.TCPServer(("", args.port), HTTPHandler) as httpd:
        print("serving at port", args.port)
        httpd.serve_forever()


# --- prepare and download

@arguments("dataset", help="The dataset ID")
@command
def prepare(args, pargs):
    dataset = Dataset.find(pargs.dataset)
    # Now, do something
    handler = dataset.getHandler()
    handler.download()
    handler.prepare()

@arguments("dataset", help="The dataset ID")
@command
def download(config, args):
    dataset = Dataset.find(config, args.dataset)

    # Now, do something
    handler = dataset.getHandler(config)
    handler.download()

@arguments("dataset", help="The dataset ID")
@command
def info(config, args):
    dataset = Dataset.find(config, args.dataset)
    handler = dataset.getHandler(config)
    print(handler.description())


# --- Search

@command
def search(args):
    cpath = configpath(args)

    for root, dirs, files in os.walk(cpath, topdown=False):
        for relpath in files:
            if relpath.endswith(YAML_SUFFIX):
                path = op.join(root, relpath)
                prefix = op.relpath(path, cpath)[:-len(YAML_SUFFIX)].replace("/", ".")
                data = readyaml(path)
                if data is not None and "data" in data:
                    for d in data["data"]:
                        if type(d["id"]) == list:
                            for _id in d["id"]:
                                print("%s.%s" % (prefix, _id))
                        else:
                            print("%s.%s" % (prefix, d["id"]))


# --- Create the argument parser

parser = argparse.ArgumentParser(description='datasets manager')
parser.add_argument("--verbose", action="store_true", help="Be verbose")
parser.add_argument("--debug", action="store_true", help="Be even more verbose")
parser.add_argument("--configuration", help="Directory containing the configuration files", default=MAINDIR)

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
    fname = "command_%s" % args.command.replace("-", "_")
    config = Configuration(args.configuration)
    commands[args.command](config, args)
except Exception as e:
    sys.stderr.write("Error while running command %s:\n" % args.command)
    sys.stderr.write(str(e))

    if args.debug:
        import traceback
        sys.stderr.write(traceback.format_exc())
