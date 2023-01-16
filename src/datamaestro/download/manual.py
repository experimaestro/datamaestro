from datamaestro.utils import deprecated
from .links import linkfolder as _linkfolder

linkfolder = deprecated(
    "@linkfolder has been moved to datamaestro.download.links", _linkfolder
)
