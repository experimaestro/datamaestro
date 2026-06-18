import functools
import http.server
import socketserver
import threading
from pathlib import Path

import datamaestro.download.single as single
from datamaestro.definitions import AbstractDataset
from .conftest import MyRepository


TEST_PATH = Path(__file__).parent


class Dataset(AbstractDataset):
    def __init__(self, repository):
        super().__init__(repository)
        self.datapath = Path(repository.context._path)

    def _prepare(self):
        pass


def test_filedownloader(context, tmp_path):
    # Serve a local file to avoid depending on flaky external services
    content = b"<html><body>hello</body></html>"
    (tmp_path / "page.html").write_bytes(content)

    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(tmp_path)
    )
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            repository = MyRepository(context)
            dataset = Dataset(repository)

            url = f"http://127.0.0.1:{port}/page.html"
            downloader = single.filedownloader("test", url)
            downloader(dataset)
            downloader.download()
        finally:
            httpd.shutdown()
            thread.join()
