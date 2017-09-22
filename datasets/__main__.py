#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import argparse
import sys
import logging
import http.server
import os.path as op
import importlib

YAML_SUFFIX = ".yaml"

share_dir = "/Users/bpiwowar/development/datasets/share"


# --- Definition of commands

import os
from os.path import join
import yaml

def readyaml(path):
    with open(path) as f:
        return yaml.load(f)

def configpath(args):
    return join(args.configuration, "config")

def datapath(args):
    return join(args.configuration, "data")

def webpath(args):
    return join(op.dirname(args.configuration), "www")


class HTTPHandler(http.server.SimpleHTTPRequestHandler):
    pass

def command_serve(args):
    import socketserver

    os.chdir(webpath(args))
    with socketserver.TCPServer(("", args.port), HTTPHandler) as httpd:
        print("serving at port", args.port)
        httpd.serve_forever()


# --- Data


class Dataset:
    def __init__(self, content):
        self.content = content

    def getHandler(self):
        name = self.content["handler"]
        logging.debug("Searching for handler %s", name)
        package, name = name.split("/")
        name = name[0].upper() + name[1:]
        
        package = importlib.import_module("datasets." + package, package="")
        return getattr(package, name)(self.content)


class DataFile:
    def __init__(self, prefix, path):
        self.content = readyaml(path)
        self.datasets = {}
        for d in self.content["data"]:
            if type(d["id"]) == list:
                for _id in d["id"]:
                    self.datasets[_id] = Dataset(d)
            else:
                self.datasets[d["id"]] = Dataset(d)

        logging.debug("Found datasets: %s", ", ".join(self.datasets.keys()))

    def __contains__(self, name):
        return name in self.datasets

    def __getitem__(self, name):
        return self.datasets[name]

# --- Prepare

commands = {}
def command(m):
    commands[m.__name__] = m
    return m

@command
def process(args):
    p = argparse.ArgumentParser("process")
    p.add_argument("dataset", help="The dataset ID")
    p.add_argument("args", nargs=argparse.REMAINDER, help="Arguments for the preparation")
    pargs = p.parse_args(args.args)

    # First, find the dataset
    logging.debug("Searching dataset %s" % pargs.dataset)
    path = configpath(args)
    components = pargs.dataset.split(".")
    sub = None
    prefix = None
    for i, c in enumerate(components):
        path = op.join(path, c)    
        if op.isfile(path + YAML_SUFFIX):
            prefix = ".".join(components[:i])
            sub = ".".join(components[i+1:])
            path += YAML_SUFFIX
            break
        if not op.isdir(path):
            raise OSError("Path {} does not exist".format(path))

    # Get the dataset
    logging.debug("Found file %s [prefix=%s/id=%s]", path, prefix, sub)
    f = DataFile(prefix, path)
    if not sub in f:
        raise Exception("Could not find the dataset %s in %s" % (args.dataset, path))
    dataset = f[sub]

    # Now, do something
    handler = dataset.getHandler()
    handler.handle(pargs.args)

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

# import importlib
# import yaml
# trec = importlib.import_module("datasets.trec.adhoc", package="")
# warc = importlib.import_module("datasets.warc", package="")
#
# with open("/Users/bpiwowar/development/datasets/share/trec/documents.yaml") as f:
#     data = warc.Handler(data.Context("trec"), yaml.load(f))


# --- Create the argument parser

parser = argparse.ArgumentParser(description='datasets manager')
parser.add_argument("--verbose", action="store_true", help="Be verbose")
parser.add_argument("--debug", action="store_true", help="Be even more verbose")
parser.add_argument("--configuration", help="Directory containing the configuration files", default=share_dir)

parser.add_argument("command", choices=commands.keys())
parser.add_argument("args", nargs=argparse.REMAINDER)

        
# subparsers = parser.add_subparsers(help='sub-command help', dest='command',)

# subparsers.add_parser("info", help="Information about ircollections")
# subparsers.add_parser("search", help="Search all the registered datasets")
# p_serve = subparsers.add_parser("serve", help="Web server to visualize datasets")
# p_serve.add_argument("--port", type=int, default=8000, help="Port for web server (default: %(default)s)")

# prepare_parser = subparsers.add_parser("prepare", help="Prepare a dataset", add_help=False)
# get_parser = subparsers.add_parser("get", help="Prepare a dataset")
# for p in [prepare_parser, get_parser]:
#     p.add_argument("dataset", help="The dataset ID")
#     p.add_argument("args", nargs="*", help="Arguments for the preparation")


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
    commands[args.command](args)
except Exception as e:
    sys.stderr.write("Error while running command %s:\n" % args.command)
    sys.stderr.write(str(e))

    if args.debug:
        import traceback
        sys.stderr.write(traceback.format_exc())
