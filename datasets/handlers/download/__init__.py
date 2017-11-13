class DownloadHandler:
    def __init__(self, repository, definition):
        self.repository = repository
        self.definition = definition

    @staticmethod
    def find(repository, definition):
        return repository.findhandler("download", definition["handler"])(repository, definition)