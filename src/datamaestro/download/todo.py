from datamaestro.download import Resource


class Todo(Resource):
    """Placeholder resource indicating download is not yet implemented."""

    def download(self, force=False):
        raise NotImplementedError(
            "Download method not defined - please edit the definition file"
        )

    def prepare(self):
        raise NotImplementedError(
            "Prepare method not defined - please edit the definition file"
        )
