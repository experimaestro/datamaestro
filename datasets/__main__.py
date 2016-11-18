#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import argparse
import sys
import logging

# Try to import argcomplete
try: from argcomplete import autocomplete
except: autocomplete = lambda x: None 

share_dir = "/Users/bpiwowar/development/datasets/share"

# --- Create the argument parser

parser = argparse.ArgumentParser(description='datasets manager')
parser.add_argument("--verbose", action="store_true", help="Be verbose")
parser.add_argument("--debug", action="store_true", help="Be even more verbose")
parser.add_argument("--configuration", help="Directory containing the configuration files", default=share_dir)

subparsers = parser.add_subparsers(help='sub-command help', dest='command')

subparsers.add_parser("info", help="Information about ircollections")
subparsers.add_parser("search", help="Search all the registered datasets")

prepare_parser = subparsers.add_parser("prepare", help="Prepare a dataset")
get_parser = subparsers.add_parser("get", help="Prepare a dataset")
for p in [prepare_parser, get_parser]:
    prepare_parser.add_argument("dataset", nargs=1, help="The dataset ID")
    prepare_parser.add_argument("args", nargs="*", help="Arguments for the preparation")

autocomplete(parser)
args = parser.parse_args()

if args.command is None:
    parser.print_help()
    sys.exit()

if args.verbose:
    logging.getLogger().setLevel(logging.INFO)

if args.debug:
    logging.getLogger().setLevel(logging.DEBUG)

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

    
def command_search(args):
    cpath = configpath(args)
    for root, dirs, files in os.walk(cpath, topdown=False):
        index = join(root, "index.yaml")
        prefix = os.path.relpath(root, cpath)
        if os.path.exists(index):
            index = readyaml(index)
            if "files" in index:
                for relpath in index["files"]:
                    path = join(root, "%s.yaml" % relpath)
                    data = readyaml(path)
                    if data is not None and "data" in data:
                        for d in data["data"]:
                            if type(d["id"]) == list:
                                for _id in d["id"]:
                                    print("%s.%s" % (prefix, _id))
                            else:
                                print("%s.%s" % (prefix, d["id"]))
                    else:
                        logging.warn("No data defined in %s" % path)

# import importlib
# import yaml
# trec = importlib.import_module("datasets.trec.adhoc", package="")
# warc = importlib.import_module("datasets.warc", package="")
#
#
# with open("/Users/bpiwowar/development/datasets/share/trec/documents.yaml") as f:
#     data = warc.Handler(data.Context("trec"), yaml.load(f))


try:
    fname = "command_%s" % args.command.replace("-", "_")
    f = globals()[fname]
    f(args)
except Exception as e:
    sys.stderr.write("Error while running command %s:\n" % args.command)
    sys.stderr.write(str(e))

    if args.debug:
        import traceback
        sys.stderr.write(traceback.format_exc())