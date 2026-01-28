# Data Types

Data types define the structure of dataset contents. They inherit from `datamaestro.data.Base`
and use experimaestro's configuration system for type-safe parameter handling.

## Base Types

### Base

The root class for all data types:

```python
from datamaestro.data import Base

class MyData(Base):
    """Custom data type"""
    pass
```

```{eval-rst}
.. autoxpmconfig:: datamaestro.data.Base
```

### Generic

Generic data with a path:

```{eval-rst}
.. autoxpmconfig:: datamaestro.data.Generic
```

### File

Single file reference:

```python
from datamaestro.data import File

# In dataset definition
return File(path=downloaded_path)

# Usage
print(ds.path)  # Path to the file
```

```{eval-rst}
.. autoxpmconfig:: datamaestro.data.File
```

## CSV Data

Package: `datamaestro.data.csv`

### Generic CSV

```python
from datamaestro.data.csv import Generic

return Generic(
    path=csv_path,
    separator=",",
    names_row=0,  # Header row index
    size=1000,    # Number of rows (optional)
)
```

```{eval-rst}
.. autoxpmconfig:: datamaestro.data.csv.Generic
```

### Matrix CSV

For numeric CSV data:

```python
from datamaestro.data.csv import Matrix

return Matrix(
    path=csv_path,
    separator=",",
    target=-1,  # Target column index (-1 for last)
)
```

```{eval-rst}
.. autoxpmconfig:: datamaestro.data.csv.Matrix
```

## Tensor Data

Package: `datamaestro.data.tensor`

### IDX Format

The IDX format is used by MNIST and similar datasets:

```python
from datamaestro.data.tensor import IDX

idx_data = IDX(path=idx_file_path)

# Load as numpy array
array = idx_data.data()
print(array.shape)  # e.g., (60000, 28, 28)
print(array.dtype)  # e.g., uint8
```

```{eval-rst}
.. autoxpmconfig:: datamaestro.data.tensor.IDX
```

## Machine Learning

Package: `datamaestro.data.ml`

### Supervised Learning

For supervised learning datasets with train/test splits:

```python
from datamaestro.data.ml import Supervised

return Supervised(
    train=train_data,
    test=test_data,
    validation=validation_data,  # Optional
)
```

```{eval-rst}
.. autoxpmconfig:: datamaestro.data.ml.Supervised
```

## HuggingFace Integration

Package: `datamaestro.data.huggingface`

For datasets from the HuggingFace Hub:

```python
from datamaestro.data.huggingface import DatasetDict

return DatasetDict(
    dataset_id="squad",
    config=None,  # Optional config name
)
```

## Creating Custom Data Types

### Basic Custom Type

Create custom data types by inheriting from {py:class}`~datamaestro.data.Base`.
Use `Param` from experimaestro to define typed parameters:

```python
from pathlib import Path
from experimaestro import Param
from datamaestro.data import Base

class TextCorpus(Base):
    """A text corpus with documents"""

    path: Param[Path]
    """Path to the corpus directory"""

    encoding: Param[str] = "utf-8"
    """Text encoding"""

    def documents(self):
        """Iterate over documents"""
        for file in self.path.glob("*.txt"):
            yield file.read_text(encoding=self.encoding)

    def __len__(self):
        return len(list(self.path.glob("*.txt")))
```

### Nested Data Types

```python
from experimaestro import Param
from datamaestro.data import Base

class LabelledData(Base):
    """Data with labels"""

    data: Param[Base]
    """The actual data"""

    labels: Param[Base]
    """The labels"""

class ImageClassification(Base):
    """Image classification dataset"""

    train: Param[LabelledData]
    """Training split"""

    test: Param[LabelledData]
    """Test split"""

    num_classes: Param[int]
    """Number of classes"""
```

### With Data Loading Methods

```python
from pathlib import Path
from experimaestro import Param
from datamaestro.data import Base

class JSONLData(Base):
    """JSON Lines format data"""

    path: Param[Path]

    def __iter__(self):
        """Iterate over records"""
        import json
        with open(self.path) as f:
            for line in f:
                yield json.loads(line)

    def to_pandas(self):
        """Load as pandas DataFrame"""
        import pandas as pd
        return pd.read_json(self.path, lines=True)

    def to_list(self):
        """Load all records into a list"""
        return list(self)
```

### Inheriting from Existing Types

```python
from datamaestro.data.csv import Matrix

class ClassificationMatrix(Matrix):
    """CSV matrix for classification tasks"""

    num_classes: Param[int]
    """Number of target classes"""

    class_names: Param[list] = None
    """Optional class names"""

    def get_class_name(self, index: int) -> str:
        if self.class_names:
            return self.class_names[index]
        return str(index)
```

## Type Annotations with Experimaestro

Data types use experimaestro's annotation system ({py:class}`~experimaestro.Param`,
{py:class}`~experimaestro.Option`, {py:class}`~experimaestro.Meta`):

```python
from experimaestro import Param, Option, Meta
from datamaestro.data import Base

class MyData(Base):
    # Required parameter
    path: Param[Path]

    # Optional parameter with default
    encoding: Param[str] = "utf-8"

    # Option (not serialized, for runtime configuration)
    cache_size: Option[int] = 1000

    # Metadata (not part of configuration identity)
    description: Meta[str] = ""
```

See the [experimaestro documentation](https://experimaestro-python.readthedocs.io/)
for more details on the configuration system.
