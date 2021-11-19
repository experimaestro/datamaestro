#!/usr/bin/env python3

import sys
import logging
from functools import update_wrapper
import traceback as tb
import pkg_resources
import re
from pathlib import Path
import shutil
from .context import Context
from typing import Set
import datamaestro

import click

logging.basicConfig(level=logging.INFO)


class Config:
    def __init__(self, context: Context):
        self.context = context
        self.traceback = False
        self.host = None


def pass_cfg(f):
    """Pass configuration information"""

    @click.pass_context
    def new_func(ctx, *args, **kwargs):
        return ctx.invoke(f, ctx.obj, *args, **kwargs)

    return update_wrapper(new_func, f)


# Get all the available repositories

REPOSITORIES = {}
for entry_point in pkg_resources.iter_entry_points("datamaestro.repositories"):
    REPOSITORIES[entry_point.name] = entry_point


# --- Create the argument parser


@click.group()
@click.option("--quiet", is_flag=True, help="Be quiet")
@click.option("--keep-downloads", is_flag=True, help="Keep downloads")
@click.option("--debug", is_flag=True, help="Be even more verbose (implies traceback)")
@click.option("--host", type=str, help="Remote hostname", default=None)
@click.option(
    "--pythonpath",
    type=str,
    help="Remote python path (default python)",
    default="python",
)
@click.option(
    "--traceback", is_flag=True, help="Display traceback if an exception occurs"
)
@click.option(
    "--data", type=Path, help="Directory containing datasets", default=Context.MAINDIR
)
@click.pass_context
def cli(ctx, quiet, debug, traceback, data, keep_downloads, host, pythonpath):
    if quiet:
        logging.getLogger().setLevel(logging.WARN)
    elif debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if host:
        context = Context.remote(host, pythonpath, data)
    else:
        context = Context(data)

    context.keep_downloads = keep_downloads
    context.traceback = traceback

    ctx.obj = Config(context)
    ctx.obj.traceback = traceback
    ctx.obj.debug = debug
    ctx.obj.host = host


def main():
    cli(obj=None)


@cli.command(help="Prints the full information about a dataset")
@click.argument("dataset", type=str)
@pass_cfg
def info(config: Config, dataset):
    from datamaestro.definitions import AbstractDataset

    dataset = AbstractDataset.find(dataset)
    print(dataset.name)
    if dataset.url:
        print(dataset.url)

    print(
        "Types (ids):",
        ", ".join(str(s.__getxpmtype__().identifier) for s in dataset.ancestors()),
    )
    print(
        "Types (class):",
        ", ".join(
            str(s.__module__ + "." + s.__qualname__) for s in dataset.ancestors()
        ),
    )
    if dataset.tags:
        print("Tags:", ", ".join(dataset.tags))
    if dataset.tasks:
        print("Tasks:", ", ".join(dataset.tasks))

    print()
    if dataset.description:
        print(dataset.description)
    else:
        print("(no description provided)")


# --- General information


@cli.command(help="List available repositories")
def repositories():
    for name, entry_point in REPOSITORIES.items():
        repo_class = entry_point.load()
        print("%s: %s" % (entry_point.name, repo_class.DESCRIPTION))


@cli.command(help="Get version")
def version():
    print(datamaestro.__version__)


# --- Cleanup


@click.option("--size", is_flag=True, help="Show size")
@cli.command(help="List (and remove) orphan directories")
@pass_cfg
def orphans(config: Config, size):
    import subprocess

    for repository in config.context.repositories():
        # For each repository
        paths = set()
        ancestors: Set[Path] = set()
        for dataset in repository:
            if dataset.hasfiles():
                paths.add(dataset.datapath)
                ancestor = dataset.datapath.parent
                while ancestor not in ancestors:
                    ancestors.add(ancestor)
                    ancestor = ancestor.parent

        def lookup(path, prefix=[]):
            if path in paths:
                return
            if path not in ancestors:
                if size:
                    print(
                        subprocess.check_output(["du", "-hs", path.absolute()])
                        .decode("utf-8")
                        .strip(),
                    )
                else:
                    print(path)
                return

            for child in path.iterdir():
                if child.is_dir():
                    lookup(child, prefix + [child.name])
                else:
                    return True

        lookup(repository.datapath)


# --- Manage external data folders


@cli.group(help="Manage external data folders")
def datafolders():
    pass


@datafolders.command("list", help="List of external data folders")
@pass_cfg
def datafolder_list(config: Config):
    for key, value in config.context.settings.datafolders.items():
        print("%s\t%s" % (key, value))


@click.argument("path", type=Path)
@click.argument("key", type=str)
@datafolders.command("set", help="List of external data folders")
@pass_cfg
def datafolder_set(config: Config, key: str, path: Path):
    settings = config.context.settings
    settings.datafolders[key] = path
    settings.save()


# --- Create a dataset

DATASET_REGEX = re.compile(r"^\w[\w\.-]+\w$")
from urllib.parse import urlparse


def dataset_id_check(ctx, param, value):
    try:
        url = urlparse(value)
        if url.scheme:
            return ".".join(url.hostname.split(".")[::-1] + url.path[1:].split("/"))
    except:
        raise

    if not DATASET_REGEX.match(value):
        raise click.BadParameter(
            "Dataset ID needs to be an URL or in the format AAA.BBBB.CCC[.DDD]"
        )
    return value


@click.argument("dataset-id", callback=dataset_id_check)
@click.argument("repository-id", type=click.Choice(REPOSITORIES.keys()))
@cli.command()
@pass_cfg
def create_dataset(config: Config, repository_id: str, dataset_id: str):
    """Create a new dataset in the repository repository-id"""
    # Construct the path of the new dataset definition
    repo_class = REPOSITORIES[repository_id].load()
    path = repo_class(config).configdir  # type: Path
    names = dataset_id.split(".")
    for name in names:
        path = path / name
    path = path.with_suffix(".py")

    if path.is_file():
        print("File {} already exists - not overwritting".format(path))
        sys.exit(1)

    template_path = Path(__file__).parent / "templates" / "dataset.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(template_path, path)
    print("Created file {}".format(path))


# --- prepare and download


@click.argument("dataset")
@cli.command()
@pass_cfg
def download(config: Config, dataset):
    """Download a dataset"""
    from datamaestro.definitions import AbstractDataset

    dataset = AbstractDataset.find(dataset)
    success = dataset.download()
    if not success:
        logging.error("One or more errors occured while downloading the dataset")
        sys.exit(1)


@click.argument("datasetid")
@click.option(
    "--encoder",
    help="Encoder used for output",
    default="normal",
    type=click.Choice(["normal", "xpm"]),
)
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

    try:
        print(dataset.format(encoder))
    except Exception as e:
        if config.traceback:
            tb.print_exc()
        logging.error("Error encoding to JSON: %s", e)
        sys.exit(1)


# --- Search


@click.argument("searchterms", nargs=-1)  # , description="Search terms (e.g. tag:XXX)")
@cli.command(help="Search for a dataset")
@pass_cfg
def search(config: Config, searchterms):
    """Search for a dataset

    Repositories can be searched with repo:REGEX expression
    Tags can be searched with tag:REGEX expression
    Tasks can be searched with task:REGEX expression

    When no search modifier `MODIFIER:` is given, matches the dataset

    """
    from .search import Condition, AndCondition

    condition = AndCondition()
    for searchterm in searchterms:
        condition.append(Condition.parse(searchterm))

    logging.debug("Search: %s", condition)
    for dataset in config.context.datasets():
        if condition.match(dataset):
            print("[%s] %s" % (dataset.repository.id, dataset.id))
