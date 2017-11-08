# Introduction

[![Documentation Status](https://readthedocs.org/projects/datasets/badge/?version=latest)](https://readthedocs.org/projects/datasets/?badge=latest)

This projects aims at grouping utilities to deal with the numerous and heterogenous datasets present on the Web. It aims
at being

1. a reference for available resources
1. a tool to automatically download and/or process resources


Each datasets is uniquely identified by a qualified name such as `nist.trec.2009.web.adhoc`. A dataset can reference other datasets.

This software integrates with the [experimaestro](http://experimaestro.sf.net) experiment manager.

## Datasets Definitions

Datasets themselves are stored in different repositories whose list is given in [repositories.yaml](repositories.yaml)


# YAML syntax

Each dataset (or a set of related datasets) is described by a YAML file. We describe now
the general YAML syntax; it can be extended by specific defitions.

- id
