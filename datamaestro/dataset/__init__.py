"""
Datasets handler
"""

from datamaestro.experimaestro import XPMNS, Type, Argument

@Argument("id", type=str, help="The unique identifier of this dataset")
@Type(XPMNS.dataset)
class Dataset:
    """Base class for all dataset objects
    
    Datasets are generated from DatasetDefinition
    """
    pass