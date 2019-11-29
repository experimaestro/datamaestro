#!/usr/bin/env python3

import argparse
import sys
import logging
import os.path as op
from functools import update_wrapper
import traceback as tb
from collections import namedtuple
import pkg_resources
import re
from pathlib import Path
import shutil
from .definitions import DatasetDefinition
from .context import Context

import click

logging.basicConfig(level=logging.INFO)

class Config: 
    def __init__(self, context: Context):
        self.context = context


def pass_cfg(f):
    """Pass configuration information"""
    @click.pass_context
    def new_func(ctx, *args, **kwargs):
        return ctx.invoke(f, ctx.obj, *args, **kwargs)
    return update_wrapper(new_func, f)

# Get all the available repositories

REPOSITORIES = {}
for entry_point in pkg_resources.iter_entry_points('datamaestro.repositories'):
    REPOSITORIES[entry_point.name] = entry_point



# --- Create the argument parser

@click.group()
@click.option("--quiet", is_flag=True, help="Be quiet")
@click.option("--keep-downloads", is_flag=True, help="Keep downloads")
@click.option("--debug", is_flag=True, help="Be even more verbose (implies traceback)")
@click.option("--traceback", is_flag=True, help="Display traceback if an exception occurs")
@click.option("--data", type=click.Path(exists=True), help="Directory containing datasets", default=Context.MAINDIR)
@click.pass_context
def cli(ctx, quiet, debug, traceback, data, keep_downloads):
    if quiet:
        logging.getLogger().setLevel(logging.WARN)
    elif debug:
        logging.getLogger().setLevel(logging.DEBUG)

    context = Context(data)
    ctx.obj = Config(context)
    context.traceback = traceback
    ctx.obj.traceback = traceback
    ctx.obj.debug = debug
    context.keep_downloads = keep_downloads

def main():
    cli(obj=None)

@cli.command(help="Prints the full information about a dataset")
@click.argument("dataset", type=str)
@pass_cfg
def info(config: Config, dataset):
    dataset = DatasetDefinition.find(dataset)
    print(dataset.description)
    print(dataset.tags)

# --- General information


@cli.command(help="List available repositories")
def repositories():
    for name, entry_point in REPOSITORIES.items():
        repo_class = entry_point.load()
        print("%s: %s" % (entry_point.name, repo_class.DESCRIPTION))


# --- Create a dataset

DATASET_REGEX = re.compile(r"^\w[\w\.-]+\w$")
from urllib.parse import urlparse
def dataset_id_check(ctx, param, value):
    try:
        value = urlparse(value)
        return ".".join(value.hostname.split(".")[::-1] + value.path[1:].split("/"))
    except:
        raise

    if not DATASET_REGEX.match(value):
        raise click.BadParameter('Dataset ID needs to be an URL or in the format AAA.BBBB.CCC[.DDD]')
    return value


@click.argument("dataset-id", callback=dataset_id_check)
@click.argument("repository-id", type=click.Choice(REPOSITORIES.keys()))
@cli.command(help="Create a new dataset in the repository repository-id")
@pass_cfg
def create_dataset(config: Config, repository_id: str, dataset_id: str):
    # Construct the path of the new dataset definition
    repo_class = REPOSITORIES[repository_id].load()
    path = repo_class(config).configdir # type: Path
    names = dataset_id.split(".")
    for name in names:
        path = path / name
    path = path.with_suffix(".yaml")

    if path.is_file():
        print("File {} already exists - not overwritting".format(path))
        sys.exit(1)

    template_path = Path(__file__).parent / "templates" / "dataset.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(template_path, path)
    print("Created file {}".format(path))

# --- prepare and download

@click.argument("dataset")
@cli.command()
@pass_cfg
def download(config: Config, dataset):
    """Download a dataset"""
    dataset = DatasetDefinition.find(dataset, context=config.context)
    success = dataset.download()
    if not success:
        logging.error("One or more errors occured while downloading the dataset")
        sys.exit(1)


@click.argument("datasetid")
@click.option("--encoder", help="Encoder used for output", default="normal", type=click.Choice(['normal', 'xpm']))
@click.option("--no-downloads", is_flag=True, help="Do not try to download datasets")
@cli.command(help="Downloads a dataset (if freely available)")
@pass_cfg
def prepare(config: Config, datasetid, encoder, no_downloads):
    """Download a dataset and returns information in json format"""
    dataset = config.context.dataset(datasetid)
    
    if not no_downloads:
        success = dataset.download()
        if not success:
            logging.error("One or more errors occured while downloading the dataset")
            sys.exit(1)

    s = dataset.prepare()
    try: 
        if encoder == "normal":
            from .utils import JsonEncoder
            print(JsonEncoder().encode(s))
        elif encoder == "xpm":
            from .utils import XPMEncoder
            print(XPMEncoder().encode(s))
        else:
            raise Exception("Unhandled encoder: {encoder}")
    except:
        if config.traceback:
            tb.print_exc()     
        logging.error("Error encoding to JSON: %s", s)
        sys.exit(1)
    
  


# --- Search

@click.argument("searchterms",  nargs=-1) #, description="Search terms (e.g. tag:XXX)")
@cli.command(help="Search for a dataset")
@pass_cfg
def search(config: Config, searchterms):
    from .search import Condition, AndCondition

    condition = AndCondition()
    for searchterm in searchterms:
        condition.append(Condition.parse(searchterm))

    for dataset in config.context.datasets():
        if condition.match(dataset):
            print(dataset.id)


