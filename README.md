# Introduction

[![Documentation Status](https://readthedocs.org/projects/datasets/badge/?version=latest)](https://readthedocs.org/projects/datasets/?badge=latest)

This projects aims at grouping utilities to deal with the numerous and heterogenous datasets, as for example in Information Retrieval tasks.
For each type of task, it tries to automate/standardize common operations like downloading topics, assessments or evaluating.

- Each task is uniquely identified by an ID, e.g. `ir/trec/2009/web/adhoc`
- Each task is associated to a definition containing all the necessary information; tasks can be output in JSON
- Resources (e.g. assessments or topics, when available online) can be automatically processed
- Resources can be transformed before being fed to a particular software (e.g. [Indri](http://www.lemurproject.org/indri/))
- Integrates with the [experimaestro](http://experimaestro.sf.net) experiment manager

*Note that this is beta software*, in particular the JSON format is still subject to change. Please contact me if you use the software so I can keep you in the loop when doing so.


