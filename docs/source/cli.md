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
datamaestro search [SEARCHTERMS]...
```

**Search Syntax:**

| Prefix | Description | Example |
|--------|-------------|---------|
| (none) | Match dataset ID (regex) | `mnist` |
| `tag:` | Match tags (regex) | `tag:classification` |
| `task:` | Match tasks (regex) | `task:image` |
| `repo:` or `repository:` | Match repository ID (regex) | `repo:image` |
| `type:` | Match data type identifier | `type:datamaestro.data.ml.Supervised` |

Multiple terms are combined with AND logic.

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

List orphan directories (downloaded data not associated with any dataset).

```bash
datamaestro orphans [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--size` | Show disk usage for each orphan |

Useful for cleaning up disk space after dataset definitions change.

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
