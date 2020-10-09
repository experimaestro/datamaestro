# See documentation on http://experimaestro.github.io/datamaestro/

from datamaestro.definitions import datatasks, datatags, dataset
from datamaestro.data import Base


@datatags("tag1", "tag2")
@datatasks("task1", "task2")
@dataset(
    Base, url="__URL__",
)
def __IDENTIFIER__():
    """Line description

  Long description
  """
    return {}
