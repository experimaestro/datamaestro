from datamaestro.annotations.agreement import useragreement
from datamaestro.definitions import AbstractDataset
from .conftest import repository


def test_useragreements(context):
    # Fake dataset
    class t(AbstractDataset):
        pass

    useragreement("test")(t(None))
