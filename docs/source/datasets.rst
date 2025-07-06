Dataset definition
------------------



A dataset definition is composed of various parts:

1. ID of the dataset determined by its location
2. Meta-information: tags, tasks
3. What to download
4. How the data can be accessed in Python


Example
=======

We take the example of the MNIST image dataset as a guide. This dataset
is defined in `the image datamaestro repository <https://github.com/experimaestro/datamaestro_image>`_.

.. code-block::
    :caption: The ``datamaestro_image.config.com.lecun`` file

    from datamaestro_image.data import ImageClassification, LabelledImages, Base
    from datamaestro.data.ml import Supervised
    from datamaestro.data.tensor import IDX

    from datamaestro.download.single import filedownloader
    from datamaestro.definitions import dataset


    @filedownloader("train_images.idx", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz")
    @filedownloader("train_labels.idx", "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz")
    @filedownloader("test_images.idx", "http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz")
    @filedownloader("test_labels.idx", "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz")
    @dataset(
        ImageClassification,
        url="http://yann.lecun.com/exdb/mnist/",
    )
    def MNIST(train_images, train_labels, test_images, test_labels):
    """The MNIST database

    The MNIST database of handwritten digits, available from this page, has a
    training set of 60,000 examples, and a test set of 10,000 examples. It is a
    subset of a larger set available from NIST. The digits have been
    size-normalized and centered in a fixed-size image.
    """
    return {
        "train": LabelledImages(
            images=IDX(path=train_images),
            labels=IDX(path=train_labels)
        ),
        "test": LabelledImages(
            images=IDX(path=test_images),
            labels=IDX(path=test_labels)
        ),
    }

In the example above, the dataset ID ``com.lecun.mnist`` is determined by the module (``datamaestro_image.config.com.lecun``)
and the name of the function ``MNIST``.

``filedownloader`` returns a :py:class:`pathlib.Path`.


The `@dataset` annotation
=========================


.. autoclass:: datamaestro.definitions.dataset
