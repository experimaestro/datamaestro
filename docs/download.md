# Download handlers

A handler can be specified using

`module/subpackage:class`

will map to class `class` in `<module>.handlers.<handlertype>.subpackage`

Two shortcuts can be used:
- `/subpackage:class`: `<module>` = datamaestro
- `subpackage:class`: `<module>` = repository module



## Single file

### /single:File

Parameters:

- `url` The URL to download
- `name` (*optional*): name of the file
- `transforms`: list of transformations

## Set of files

### /multiple:List

A set of files or folders, each being handled by its own download handler.

??? example "Usage"
    This shows how to specify three files
    
    ``` yaml
    download: !@/multiple:List
        train: !@/single:File
            url: https://s3.amazonaws.com/my89public/quac/train_v0.2.json
        test: !@/single:File
            url: https://s3.amazonaws.com/my89public/quac/val_v0.2.json
        scorer: !@/single:File
            description: Evaluation script 
            url: https://s3.amazonaws.com/my89public/quac/scorer.py
    ```

### /archive:Zip

Parameters:

- `url`: link to the ZIP archive

### /archive:Tar

Transparent decompression of the archive

- `url`: link to the TAR archive
