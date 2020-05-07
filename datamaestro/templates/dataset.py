# See documentation on http://experimaestro.github.io/datamaestro/

from datamaestro.definitions import data, argument, datatasks, datatags, dataset


@datatags("tag1", "tag2")
@datatasks("task1", "task2")
@dataset(
    __DATATYPE__, url="__URL__",
)
def __IDENTIFIER__():
    """Line description

  Long description
  """
    return {}
