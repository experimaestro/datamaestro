import logging
from typing import Optional

from datamaestro.download import Download


class hf_download(Download):
    """Use Hugging Face to download a file"""

    def __init__(
        self,
        varname: str,
        repo_id: str,
        *,
        data_files: Optional[str] = None,
        split: Optional[str] = None
    ):
        """Use

        Args:
            varname: Variable name
            repo_id: The HuggingFace repository ID
        """
        super().__init__(varname)
        self.repo_id = repo_id
        self.data_files = data_files
        self.split = split

    def download(self, force=False):
        try:
            from datasets import load_dataset
        except ModuleNotFoundError:
            logging.error("the datasets library is not installed:")
            logging.error("pip install datasets")
            raise

        self.dataset = load_dataset(self.repo_id, data_files=self.data_files)
        return True

    def prepare(self):
        return {
            "repo_id": self.repo_id,
            "data_files": self.data_files,
            "split": self.split,
        }
