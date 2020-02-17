from .context import Context, Repository, prepare_dataset

from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    __version__ = None
