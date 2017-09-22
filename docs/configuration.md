# Specification

Configuration is provided by sets of `yaml` files organized hierarchically by organization name. For example,
`nist.trec.documents.clueweb09` can be found in the file `nist/trec/documents.yaml`. The path `nist/trec` defines the organization.

## Format

```yaml
data:
    -
        id: ap88
        description: Associated Press (1988)
        pattern: ^AP88.*$
        type: nist.trec.collection
```

# Storage

Datasets can be stored in various places. **datasets** will remember where.
