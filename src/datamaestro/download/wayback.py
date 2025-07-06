import logging
import json
from datamaestro.download import Resource
from typing import Callable, Iterator
from pathlib import Path
import requests
import random
import re
from requests.exceptions import HTTPError
from tqdm.auto import tqdm
import time
import urllib.parse
import uuid


wayback_prefix = re.compile(r"^https:\/\/web\.archive\.org\/web")
replace_pattern = re.compile(r"(web\.archive\.org\/web\/\d+)")


def download_with_retry(url: str, max_retries: int = 10) -> requests.Response:
    """Download a URL with exponential backoff, until max_retries is reached."""
    retry_num = 0
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except HTTPError as e:
            status_code = e.response.status_code
            if not (status_code == 429 or status_code >= 500):
                # This is not an error we should retry on
                raise e

            if retry_num > max_retries:
                logging.error(
                    f"Failed to perform GET request on {url}"
                    f"after {max_retries} retries."
                )
                raise e

            if status_code == 429:
                time.sleep(5 + 2**retry_num + random.randint(0, 1000) / 1000)
            else:
                time.sleep(2**retry_num + random.randint(0, 1000) / 1000)
            retry_num += 1


def download_link(link: str, timestamp: str):
    page_id = str(uuid.uuid4())
    url_no_header = None

    try:
        # Find the Wayback Machine link
        if not wayback_prefix.match(link):
            link_encoded = urllib.parse.quote(link)

            available, availability_attempt = False, 0
            # Sometimes the API returns HTTP success code 200, but archived
            # snapshots shows page is unavailable when it actually is. Give it a
            # total of three tries.
            while not available and availability_attempt < 3:
                response = download_with_retry(
                    "http://archive.org/wayback/available?"
                    f"url={link_encoded}&timestamp={timestamp}"
                )
                json_response = response.json()
                available = "closest" in json_response["archived_snapshots"]
                availability_attempt += 1

            if not available:
                logging.warning(
                    f"Not available on Wayback Machine: {link}, "
                    f"HTTP code {response.status_code}, {json_response}"
                )
                return {"link": link, "page_id": page_id, "available": False}

            url = json_response["archived_snapshots"]["closest"]["url"]
        else:
            url = link

        match = replace_pattern.search(url)
        assert match
        url_no_header = replace_pattern.sub(f"{match.group(1)}id_", url)

        response = download_with_retry(url_no_header)
        html_page = response.text

        return {
            "link": link,
            "id": url_no_header,
            "contents": html_page,
        }

    except HTTPError as http_err:
        logging.warning(f"HTTP error occurred: {http_err} for {link}")
        return {
            "link": link,
            "page_id": page_id,
            "available": False,
            "status_code": http_err.response.status_code if http_err.response else None,
            "wayback_url": url_no_header,
        }
    except UnicodeDecodeError as e:
        logging.warning(f"Unicode decode error occurred: {e} for {link}")
        return {
            "link": link,
            "page_id": page_id,
            "available": False,
            "status_code": response.status_code,
            "wayback_url": url_no_header,
        }
    except Exception as e:
        logging.warning(f"Exception occurred: {e} for {link}")
        return {
            "link": link,
            "page_id": page_id,
            "available": False,
            "status_code": None,
            "wayback_url": url_no_header,
        }


class wayback_documents(Resource):
    """Collect documents from wayback"""

    def __init__(self, timestamp: str, urls_fn: Callable[[], Iterator[str]], name=None):
        super().__init__(name)
        self.timestamp = timestamp
        self.urls_fn = urls_fn

    def prepare(self):
        return self.definition.datapath / self.varname

    def download(self, force=False):
        # Creates directory if needed
        destination: Path = self.definition.datapath / self.varname
        self.definition.datapath.mkdir(exist_ok=True)

        # Early exit
        done_path = destination.with_suffix(".done")
        if done_path.is_file() and not force:
            return True

        # Reads the URLs
        logging.info("Retrieving URLs from wayback into %s", destination)
        pos = 0
        urls = set()
        with destination.open("at+") as fp:
            fp.seek(0)
            try:
                while line := fp.readline():
                    pos = fp.tell()
                    urls.add(json.loads(line)["url"])
            except json.JSONDecodeError:
                logging.warning(f"JSON decoding error: getting back to position {pos}")
                fp.seek(pos)

            # Get the remaining ones
            for url in tqdm(self.urls_fn()):
                if url not in urls:
                    fp.write(json.dumps(download_link(url, self.timestamp)))

        # Everything is fine
        done_path.touch()
