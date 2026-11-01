# Command Line Interface

Datamaestro provides a command line interface for searching, downloading, and managing datasets.

## Global Options

```bash
datamaestro [OPTIONS] COMMAND [ARGS]...
```

| Option | Description |
|--------|-------------|
| `--quiet` | Suppress informational messages |
| `--debug` | Enable debug logging |
| `--traceback` | Show full traceback on errors |
| `--data PATH` | Set the data directory (default: `~/datamaestro`) |
| `--keep-downloads` | Keep downloaded archive files after extraction |
| `--host HOST` | Remote hostname for distributed operations |
| `--pythonpath PATH` | Python path on remote host (default: `python`) |

## Commands

### search

Search for datasets matching given criteria.

```bash
datamaestro search [OPTIONS] [SEARCHTERMS]...
```

| Option | Description |
|--------|-------------|
| `--refresh` | Reload all the dataset definitions before searching |
| `--errors` | List the definition modules that could not be loaded |

**Search Syntax:**

Each term is a regular expression, matched without regard to case.

| Prefix | Description | Example |
|--------|-------------|---------|
| (none) | Match the id, name, description, tags, tasks and variant values | `mnist` |
| `id:` | Match the dataset id (or an alias) | `id:^com\.lecun` |
| `tag:` | Match tags | `tag:classification` |
| `task:` | Match tasks | `task:image` |
| `repo:` or `repository:` | Match the repository id | `repo:image` |
| `type:` | Match the identifier of the data type (or one of its ancestors) | `type:datamaestro.data.ml.Supervised` |

Multiple terms are combined with AND logic.

Datasets with variants are listed once, followed by the names of their
variant axes (e.g. `org.example.corpus[lang,streaming]`); use
`datamaestro info` to see the possible values.

Only the datasets defined in the current Python environment are searched:
with uv, each project has its own environment, and thus possibly other
repositories or repository versions.

The list of datasets is cached, since building it imports all the dataset
definitions (see [Configuration](configuration.md) for its location). The
cache is rebuilt when a repository is updated (new version, or modified
definition files); `--refresh` forces a rebuild.

**Examples:**

```bash
# Find all MNIST-related datasets
datamaestro search mnist

# Find classification datasets
datamaestro search tag:classification

# Find image classification datasets in image repository
datamaestro search repo:image task:classification

# Find datasets with specific type
datamaestro search type:datamaestro.data.ml.Supervised

# Find datasets whose id starts with com.lecun
datamaestro search 'id:^com\.lecun'
```

### info

Display detailed information about a dataset.

```bash
datamaestro info DATASET
```

**Example:**

```bash
$ datamaestro info com.lecun.mnist
com.lecun.mnist
http://yann.lecun.com/exdb/mnist/
Types (ids): datamaestro_image.data.ImageClassification
Types (class): datamaestro_image.data.ImageClassification
Tags: benchmark, classification
Tasks: image-classification

The MNIST database of handwritten digits...
```

### download

Download dataset resources without preparing the data structure.

```bash
datamaestro download DATASET
```

**Example:**

```bash
datamaestro download com.lecun.mnist
```

### prepare

Download and prepare a dataset, returning JSON with paths and metadata.

```bash
datamaestro prepare [OPTIONS] DATASETID
```

| Option | Description |
|--------|-------------|
| `--encoder {normal,xpm}` | Output format (default: `normal`) |
| `--no-downloads` | Skip downloading, use existing files |

**Example:**

```bash
$ datamaestro prepare com.lecun.mnist
{
  "train": {
    "images": {"path": "/home/user/datamaestro/data/..."},
    "labels": {"path": "/home/user/datamaestro/data/..."}
  },
  ...
}
```

### repositories

List all available dataset repositories.

```bash
datamaestro repositories
```

**Example:**

```bash
$ datamaestro repositories
image: Image datasets
text: NLP and information retrieval datasets
ml: Machine learning datasets
```

### version

Display the datamaestro version.

```bash
datamaestro version
```

### orphans

List orphan directories (downloaded data not associated with any dataset
defined in the current Python environment).

```bash
datamaestro orphans [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--size` | Show disk usage for each orphan |

Useful for cleaning up disk space after dataset definitions change.

A dataset may be defined in another Python environment (e.g. another uv
project with another version of its repository). Folders of datasets known
from the registry of downloaded datasets (see `cache` below) are thus marked with the
dataset id and the repository version that last used them:

```bash
$ datamaestro orphans
/home/user/datamaestro/data/text/org/example/old  [registered: org.example.old, version 2024.1]
```

### create-dataset

Create a new dataset definition file from a template.

```bash
datamaestro create-dataset REPOSITORY_ID DATASET_ID
```

**Arguments:**

- `REPOSITORY_ID`: Target repository (e.g., `image`, `text`)
- `DATASET_ID`: Dataset identifier (e.g., `com.example.mydataset`) or URL

**Example:**

```bash
# Create from qualified ID
datamaestro create-dataset image com.example.mydataset

# Create from URL (ID is derived automatically)
datamaestro create-dataset text http://example.com/datasets/mydata
```

### serve

Start a web interface to browse the datasets. It needs the `gui` extra
(`pip install datamaestro[gui]`, which installs
[NiceGUI](https://nicegui.io)).

```bash
datamaestro serve [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--host HOST` | Address to listen on (default: `127.0.0.1`) |
| `--port PORT` | Port to listen on (default: `8080`) |
| `--refresh` | Reload all the dataset definitions first |
| `--show` | Open the interface in a web browser |

The interface offers:

- a search box (same syntax as `datamaestro search`) and facets to filter
  by repository, task and tag;
- the list of datasets, with their size on disk when they are downloaded;
- the details of a dataset: description, links, download status, and the
  code to use it (`prepare_dataset(...)` or `datamaestro prepare ...`).
  For a dataset with variants, the value of each axis can be chosen, and
  the corresponding id is shown;
- the downloaded datasets that are not defined in the current Python
  environment (status "not in this environment"), with the repository
  version that last used them;
- the definition modules that could not be loaded (e.g. because of a
  missing dependency).

The reload button (top right) rebuilds the list of datasets, e.g. after
installing a new repository.

### cache

Manage the [ds-cache-cleaner](https://pypi.org/project/ds-cache-cleaner/)
registry of downloaded datasets.

Downloaded datasets are recorded in `$DATAMAESTRO_DIR/data/ds-cache-cleaner/`
(id, name, repository and its version, definition module, tags, size and
last access), so that `ds-cache-cleaner` shows which datasets take space and
when they were last used. Datasets downloaded before a repository (or a
repository version) was known to the registry are registered automatically
on the next download.

#### cache sync

Register the downloaded datasets that are not in the registry yet, and
forget the ones whose folder was deleted.

```bash
datamaestro cache sync [--dry-run]
```

| Option | Description |
|--------|-------------|
| `--dry-run` | Only show what would change |

**Example:**

```bash
$ datamaestro cache sync
+ text/gov/nist/trec/adhoc/5/topics
1 registered, 0 removed
```

### datafolders

Manage external data folders for datasets that reference pre-existing data.

#### datafolders list

List configured data folders.

```bash
datamaestro datafolders list
```

#### datafolders set

Set an external data folder path.

```bash
datamaestro datafolders set KEY PATH
```

**Example:**

```bash
# Configure a folder for large datasets
datamaestro datafolders set large_data /mnt/storage/datasets

# List configured folders
datamaestro datafolders list
large_data    /mnt/storage/datasets
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATAMAESTRO_DIR` | Base directory for data storage | `~/datamaestro` |

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Error (download failed, dataset not found, etc.) |
