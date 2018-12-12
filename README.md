# Introduction

This projects aims at grouping utilities to deal with the numerous and heterogenous datasets present on the Web. It aims
at being

1. a reference for available resources
1. a tool to automatically download and/or process resources

Each datasets is uniquely identified by a qualified name such as `nist.trec.2009.web.adhoc`. A dataset can reference other datamaestro.


# YAML syntax

Each dataset (or a set of related datasets) is described by a YAML file. Its syntax is
described in the [documentation](http://bpiwowar.github.io/datamaestro/).

# Example

The commmand line interface allows to download automatically the different resources. Datamaestro extensions can provide additional processing tools.

```sh
$ datamaestro search glove   
Dataset(edu.standford.glove)
Dataset(edu.standford.glove.6b)
Dataset(edu.standford.glove.42b)
Dataset(edu.standford.glove.840b)

$ datamaestro prepare edu.standford.glove.6b
INFO:root:Downloading Dataset(edu.standford.glove.6b)
INFO:root:Downloading http://nlp.stanford.edu/data/glove.6B.zip into .../glove/6b
INFO:root:Downloading http://nlp.stanford.edu/data/glove.6B.zip
100% of 822.2 MiB |###############################################| Elapsed Time: 0:01:54 Time:  0:01:54
INFO:root:Unzipping file
{"id": "edu.standford.glove.6b", "path": ".../datamaestro/data/text/edu/standford/glove/6b"}

```
