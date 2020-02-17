# See documentation on http://experimaestro.github.io/datamaestro/

from datamaestro.definitions import data, argument, datatasks, datatags, dataset


@datatags("tag1", "tag2")
@datatasks("task1", "task2")
@dataset(
    DataType, url="__URL__",
)
def __IDENTIFIER__(train_images, train_labels, test_images, test_labels):
    """Line description

  Long description
  """
    return {}
