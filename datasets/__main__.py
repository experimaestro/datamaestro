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

import click

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


# --- Create the argument parser

@click.group()
@click.option("--quiet", is_flag=True, help="Be quiet")
@click.option("--debug", is_flag=True, help="Be even more verbose (implies traceback)")
@click.option("--traceback", is_flag=True, help="Display traceback if an exception occurs")
@click.option("--data", type=click.Path(exists=True), help="Directory containing datasets", default=Configuration.MAINDIR)
@click.pass_context
def cli(ctx, quiet, debug, traceback, data):
    if quiet:
        logging.getLogger().setLevel(logging.WARN)
    elif debug:
        logging.getLogger().setLevel(logging.DEBUG)


    ctx.obj = Configuration(data)

def main():
    cli(obj=None)

@cli.command()
@click.argument("dataset", type=str) #, help="The dataset ID")
@click.pass_context
def info(ctx, dataset):
    dataset = Dataset.find(ctx.obj, dataset)
    handler = dataset.getHandler()
    print(handler.description())


# --- Manage repositories

@cli.group(help="Manage repositories")
@click.pass_context
def repositories(ctx):
    pass

@repositories.command()
@click.pass_context
def list(ctx):
    import pkgutil
    data = pkgutil.get_data('datasets', 'repositories.yaml')
    repositories = yaml.load(data)
    for key, info in repositories.items():
        print(key, info["description"])


# --- prepare and download

@cli.command()
def generate_site(config: Configuration, args):
    import datasets.commands.site as site
    site.generate()


@click.argument("dataset")
@cli.command()
@click.pass_context
def download(ctx, dataset):
    dataset = Dataset.find(ctx.obj, dataset)

    # Now, do something
    handler = dataset.getHandler()
    if not handler.download():
        logging.error("One or more errors occured while downloading the dataset")
        sys.exit(1)


@click.argument("dataset")
@cli.command()
@click.pass_context
def prepare(ctx, dataset):
    dataset = Dataset.find(ctx.obj, dataset)

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



# --- Search

@click.argument("regexp")
@cli.command()
def search(config: Configuration, args):
    import re
    pattern = re.compile(args.regexp)
    for dataset in config.datasets():
        if any([pattern.search(id) is not None for id in dataset.ids]):
            print(dataset)
