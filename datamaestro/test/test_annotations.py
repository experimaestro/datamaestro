from datamaestro.annotations.agreement import useragreement
from datamaestro.definitions import DatasetDefinition
from .conftest import repository


def test_useragreements(context):
    # Fake dataset
    class t:
        pass

    t.__datamaestro__ = DatasetDefinition(repository, t)

    useragreement("test")(t)
