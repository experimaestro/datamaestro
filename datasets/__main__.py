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

from .context import Context
from .data import Dataset

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
@click.option("--data", type=click.Path(exists=True), help="Directory containing datasets", default=Context.MAINDIR)
@click.pass_context
def cli(ctx, quiet, debug, traceback, data):
    if quiet:
        logging.getLogger().setLevel(logging.WARN)
    elif debug:
        logging.getLogger().setLevel(logging.DEBUG)


    ctx.obj = Context(data)

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

@cli.group()
def site():
    pass

@site.command()
@click.pass_context
def generate(ctx):
    import datasets.commands.site as site
    site.generate(ctx.obj)


@click.argument("dataset")
@cli.command()
@click.pass_context
def download(ctx, dataset):
    dataset = Dataset.find(ctx.obj, dataset)

    # Now, do something
    handler = dataset.getHandler()
    r = handler.download()
    if not r:
        logging.error("One or more errors occured while downloading the dataset")
        sys.exit(1)


@click.argument("datasetid")
@cli.command()
@click.pass_context
def prepare(ctx, datasetid):
    dataset = ctx.obj.dataset(datasetid)
    success = dataset.download()
    if not success:
        logging.error("One or more errors occured while downloading the dataset")
        sys.exit(1)

    s = dataset.prepare()
    try: 
        print(ExperimaestroEncoder().encode(s))
    except:
        logging.error("Error encoding to JSON: %s", s)
        sys.exit(1)
    
  


# --- Search

@click.argument("regexp")
@cli.command()
@click.pass_context
def search(ctx, regexp):
    import re
    pattern = re.compile(regexp)
    for dataset in ctx.obj.datasets():
        if any([pattern.search(id) is not None for id in dataset.ids]):
            print(dataset)



@cli.command()
def test():
    from datasets.handlers.transform import Filter
    filter = Filter({ "pattern": "@click"})
    with open(__file__, "rb") as fp:
        with filter(fp) as fp2:
            print(fp2.read().decode("utf-8"))
