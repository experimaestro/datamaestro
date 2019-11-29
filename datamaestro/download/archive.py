import logging
from datamaestro.download import Download
from pathlib import Path
import zipfile
import shutil
import urllib3
import tarfile


class ArchiveDownloader(Download):
    def __init__(self, repository, definition):
        super().__init__(repository, definition)
        self.url = self.definition["url"]
        self.subpath = self.definition.get("path", None)
        self._files = self.definition.get("files", None)
        if self.subpath and not self.subpath.endswith("/"):
            self.subpath = self.subpath + "/"

    def resolve(self, path: Path) -> Path:
        """Returns the destination path"""
        p = urllib3.util.parse_url(self.url)
        return path.joinpath(Path(p.path).name)



    def download(self, destination: Path):
        # Already downloaded
        if destination.is_dir(): return 
        logging.info("Downloading %s into %s", self.url, destination)

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmpdestination = destination.with_suffix(".tmp")
        if tmpdestination.exists():
            logging.warn("Removing temporary directory %s", tmpdestination)
            shutil.rmtree(tmpdestination)

        with self.dataset.downloadURL(self.url) as file:
            self.unarchive(file, tmpdestination)

        # Look at the content
        for ix, path in enumerate(tmpdestination.iterdir()):
            if ix > 1: break
        
        # Just one file/folder: move
        if ix == 0 and path.is_dir():
            logging.info("Moving single directory {} into destination {}".format(path, destination))
            shutil.move(str(path), str(destination))
            shutil.rmtree(tmpdestination)
        else:
            shutil.move(tmpdestination, destination)


class Zip(ArchiveDownloader):
    """ZIP Archive handler"""
        
    def resolve(self, path: Path) -> Path:
        """Returns the destination path"""
        p = urllib3.util.parse_url(self.url)
        return path.joinpath(Path(p.path).name)

    def unarchive(self, file, destination: Path):
        logging.info("Unzipping file")
        with zipfile.ZipFile(file.path) as zip:
            if self.subpath is None:
                zip.extractall(destination)
            else:
                L = len(self.subpath)
                for zip_info in zip.infolist():
                    if zip_info.filename.startswith(self.subpath):
                        name = zip_info.filename[L:]
                        if zip_info.is_dir():
                            (destination / name).mkdir()
                        else:
                            logging.info("File %s (%s) to %s", zip_info.filename, name, destination /name)
                            with zip.open(zip_info) as fp, (destination / name).open("wb") as out:
                                shutil.copyfileobj(fp, out)
        


class Tar(ArchiveDownloader):
    """TAR archive handler"""

    def unarchive(self, file: Path, destination: Path):
        logging.info("Unarchiving file")
        if self.subpath: raise NotImplementedError()
        with tarfile.TarFile.open(file.path, mode="r:*") as tar:
            tar.extractall(tmpdestination)
