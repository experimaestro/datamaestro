# Specification

Configuration is provided by sets of `yaml` files organized hierarchically by organization name. For example,
`nist.trec.documents.clueweb09` can be found in the file `nist/trec/documents.yaml`. The path `nist/trec` defines the organization.

## Format

### Example

The following is a complete example that describes the Glove dataset

```yaml
website: http://nlp.stanford.edu/projects/glove/
license: Public Domain Dedication and License
tags:
  - word embeddings
  - word representation

documents:
  technical description: http://nlp.stanford.edu/pubs/glove.pdf

description: |
  GloVe is an unsupervised learning algorithm for obtaining vector representations for words.
  Training is performed on aggregated global word-word co-occurrence statistics from a corpus,
  and the resulting representations showcase interesting linear substructures of the word vector space.


data:
  6b:
    description: Trained on Wikipedia 2014 + Gigaword 5
    download: http://nlp.stanford.edu/data/glove.6B.zip
    size: 822M
    statistics:
      tokens: 6G
      vocabulary: 400K
      cased: false
      dimension: [50, 100, 200, 300]
  42b:
    description: Trained on Common Crawl
    download: http://nlp.stanford.edu/data/glove.42B.300d.zip
    size: 2.03G
    statistics:
      cased: true
      tokens: 840G
      vocabulary: 2.2M
      dimension: 300
  840b:
    description: Trained on Common Crawl
    download: http://nlp.stanford.edu/data/glove.840B.300d.zip
    size: 2.03G
    statistics:
      cased: true
      tokens: 840G
      vocabulary: 2.2M
      dimension: 300
```

## Fields

### General information

- `name` gives a short name of the dataset
- `description` gives a longer description of the dataset
- `tags` is a list of tags
- `date` is the dataset release date 

### Documents

`documents` is a dictionary that associates a name to some fields like

- `url` 
- `bibtex`

### Nested datasets

Some datasets are variations of the same dataset. In that case, `sub` gives a list 
of sub-datasets. Fields are inherited by default.

### Download and data

The field `download` specifies how data should be downloaded


