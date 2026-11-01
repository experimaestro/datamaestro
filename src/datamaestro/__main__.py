#!/usr/bin/env python3
# ruff: noqa: T201

from importlib.metadata import entry_points
import sys
import logging
from functools import update_wrapper
import traceback as tb
import re
from pathlib import Path
import shutil
from typing import Set
from urllib.parse import urlparse

import click

import datamaestro
from .context import Context

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
for entry_point in entry_points(group="datamaestro.repositories"):
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
    "--data",
    type=Path,
    help="Directory containing datasets",
    default=Context.MAINDIR,
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
    if dataset.requirements:
        missing = set(dataset.missing_requirements())
        print(
            "Requires:",
            ", ".join(
                f"{r}{' [missing]' if r in missing else ''}"
                for r in dataset.requirements
            ),
        )

    variants = getattr(dataset, "variants", None)
    if variants is not None:
        _print_variants(dataset, variants)

    print()
    if dataset.description:
        print(dataset.description)
    else:
        print("(no description provided)")


def _print_variants(dataset, variants):
    """Render a human-readable summary of a variants space."""
    from datamaestro.variants import AxesVariants

    print()
    print("Variants:")
    example_kwargs = {}
    if isinstance(variants, AxesVariants):
        axes = variants.axes
        # Required axes first, then the ones with defaults.
        order = sorted(axes, key=lambda k: (axes[k].has_default, k))
        for name in order:
            axis = axes[name]
            parts = []
            if axis.domain is not None:
                if len(axis.domain) <= 8:
                    parts.append(
                        "one of {" + ", ".join(repr(v) for v in axis.domain) + "}"
                    )
                else:
                    preview = ", ".join(repr(v) for v in axis.domain[:4])
                    parts.append(f"one of {{{preview}, … ({len(axis.domain)} values)}}")
            elif axis.type is not None:
                parts.append(f"any {_axis_type_label(axis.type)}")
            else:
                parts.append("any string")
            if axis.has_default:
                parts.append(f"default={axis.default!r}")
            else:
                parts.append("required")
            if axis.description:
                parts.append(axis.description)
            print(f"  - {name}: {'; '.join(parts)}")
            if not axis.has_default and axis.domain:
                example_kwargs.setdefault(name, axis.domain[0])
    else:
        # Non-AxesVariants subclass — just list the enumerable keys.
        for combo in variants.enumerate():
            print("  - ", ", ".join(f"{k}={v!r}" for k, v in combo.items()))
            break  # preview only

    # Build and print a concrete example selector the user can copy.
    try:
        example = variants.format_selector(example_kwargs)
    except Exception:
        example = None
    if example:
        print()
        print(f"Example: datamaestro prepare {dataset.id}{example}")


def _axis_type_label(t):
    """Pretty-print an Axis type. Handles ``Optional[T]`` nicely."""
    import typing

    origin = typing.get_origin(t)
    args = typing.get_args(t)
    if origin is typing.Union and type(None) in args:
        inner = next((a for a in args if a is not type(None)), None)
        if inner is not None:
            return f"{_axis_type_label(inner)} (or null)"
    return getattr(t, "__name__", str(t))


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
@cli.command(
    help="List orphan directories (not defined in this environment; "
    "registered ones are marked)"
)
@pass_cfg
def orphans(config: Config, size):
    """Lists the folders that do not correspond to a dataset

    Datasets defined in another Python environment (e.g. by another version
    of their repository) are only known from the ds-cache-cleaner registry:
    their folder is listed with the registered id and repository version
    """
    import subprocess

    from datamaestro import cleaner

    try:
        registered = cleaner.entries(config.context)
    except Exception:
        registered = {}

    def note(path: Path) -> str:
        try:
            relpath = path.relative_to(config.context.datapath).as_posix()
        except ValueError:
            return ""
        if (entry := registered.get(relpath)) is None:
            return ""
        metadata = entry.metadata
        version = metadata.get("repository_version")
        return (
            f"  [registered: {metadata.get('id', relpath)}"
            f"{f', version {version}' if version else ''}]"
        )

    # Registered folders are shown even when inside an orphan folder
    registered_ancestors: Set[Path] = set()
    for relpath in registered:
        ancestor = (config.context.datapath / relpath).parent
        while ancestor not in registered_ancestors and ancestor != ancestor.parent:
            registered_ancestors.add(ancestor)
            ancestor = ancestor.parent

    for repository in config.context.repositories():
        # For each repository
        paths = set()
        ancestors: Set[Path] = set()
        for dataset in repository:
            # Some datasets (e.g. from plugins) only define hasfiles()
            has_files = getattr(type(dataset), "has_files", None)
            if has_files(dataset) if has_files else dataset.hasfiles():
                paths.add(dataset.datapath)
                ancestor = dataset.datapath.parent
                while ancestor not in ancestors:
                    ancestors.add(ancestor)
                    ancestor = ancestor.parent

        def lookup(path, prefix=[]):
            if path in paths:
                return
            if path not in ancestors and path not in registered_ancestors:
                if size:
                    print(
                        subprocess.check_output(["du", "-hs", path.absolute()])
                        .decode("utf-8")
                        .strip()
                        + note(path),
                    )
                else:
                    print(f"{path}{note(path)}")
                return

            for child in path.iterdir():
                if child.is_dir():
                    lookup(child, prefix + [child.name])
                else:
                    return True

        if repository.datapath.is_dir():
            lookup(repository.datapath)


@cli.group(help="Manage the ds-cache-cleaner registry of downloaded datasets")
def cache():
    pass


@cache.command(
    "sync",
    help="Register the downloaded datasets with ds-cache-cleaner, "
    "and forget the deleted ones",
)
@click.option("--dry-run", is_flag=True, help="Only show what would change")
@pass_cfg
def cache_sync(config: Config, dry_run: bool):
    from datamaestro.cleaner import sync

    report = sync(config.context, dry_run=dry_run)
    for path in report.added:
        print(f"+ {path}")
    for path in report.removed:
        print(f"- {path}")
    print(
        f"{len(report.added)} registered, {len(report.removed)} removed"
        + (" (dry run)" if dry_run else "")
    )


@cli.command(help="Web interface to browse the datasets (needs datamaestro[gui])")
@click.option("--host", default="127.0.0.1", help="Address to listen on")
@click.option("--port", default=8080, type=int, help="Port to listen on")
@click.option(
    "--refresh", is_flag=True, help="Reload all the dataset definitions first"
)
@click.option("--show", is_flag=True, help="Open the interface in a browser")
@pass_cfg
def serve(config: Config, host: str, port: int, refresh: bool, show: bool):
    from datamaestro.gui import serve

    serve(config.context, host=host, port=port, refresh=refresh, show=show)


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


@click.argument("repository", type=click.Choice(REPOSITORIES.keys()))
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    default=None,
    help="Output JSON file (default: stdout)",
)
@cli.command()
@pass_cfg
def check(config: Config, repository, output):
    """Check if remote download sources are still available.

    Performs HEAD requests (or equivalent) to verify that URLs
    referenced by dataset resources are reachable.

    Iterates over all datasets in REPOSITORY and outputs results as JSON.
    """
    import json

    from datamaestro.download import CheckStatus

    from tqdm.auto import tqdm

    repo = config.context.repository(repository)
    datasets = list(repo)
    results = []

    for ds in tqdm(datasets, desc="Checking datasets"):
        try:
            ds.prepare()
        except Exception as e:
            logging.warning("Could not prepare %s: %s", ds.id, e)
            continue

        for resource in ds.resources.values():
            result = resource.check()
            if result.status == CheckStatus.SKIPPED:
                continue

            cls = type(resource)
            results.append(
                {
                    "dataset": ds.id,
                    "resource": result.resource,
                    "resource_type": f"{cls.__module__}.{cls.__qualname__}",
                    "status": result.status.value,
                    "url": result.url,
                    "message": result.message,
                }
            )

    json_output = json.dumps(results, indent=2)
    if output:
        Path(output).write_text(json_output)
        logging.info("Wrote %d results to %s", len(results), output)
    else:
        print(json_output)


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


@click.argument("searchterms", nargs=-1)
@click.option(
    "--refresh", is_flag=True, help="Reload all the dataset definitions first"
)
@click.option(
    "--errors", is_flag=True, help="Show the definition modules that failed to load"
)
@cli.command(help="Search for a dataset")
@pass_cfg
def search(config: Config, searchterms, refresh: bool, errors: bool):
    """Search for a dataset

    All the terms must match (case-insensitive regular expressions). A term
    can be restricted to a field with a prefix: tag:, task:, type:, repo:
    (or repository:) and id:. Other terms are matched against the id,
    name, description, tags, tasks and variant values of the datasets.

    The dataset list is cached (see --refresh).
    """
    from .catalog import Catalog

    catalog = Catalog.load(config.context, refresh=refresh)
    for entry in catalog.search(searchterms):
        suffix = ""
        if entry.variants is not None:
            axes = [axis.name for axis in entry.variants.axes or []]
            suffix = f"[{','.join(sorted(axes))}]" if axes else "[...]"
        cfg = entry.config_type.rsplit(".", 1)[-1] if entry.config_type else "?"
        print(f"[{entry.repository}] {entry.id}{suffix} ({cfg})")

    if errors:
        for error in catalog.errors:
            print(f"{error.source}: {error.error}", file=sys.stderr)
    elif catalog.errors:
        logging.warning(
            "%d definition modules could not be loaded (see --errors)",
            len(catalog.errors),
        )
