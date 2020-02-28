from datamaestro.utils import deprecated
from links import _linkfolder

linkfolder = deprecated(
    "@linkfolder has been moved to datamaestro.download.links", _linkfolder
)
