import bz2
import gzip
import json
import lzma
import os
import zlib
import psutil

from abc import ABC, abstractmethod
from logger.logger import Logging

COMPRESSION_TYPES = {"zlib": zlib, "glib": gzip, "bz2": bz2, "lzma": lzma}

PARTITION_BYTE = 100_000_000

logger = Logging(name=__name__).get_logger()


class Compressor(ABC):
    def __init__(self, path):
        self.path = path

    @abstractmethod
    def write(self, file_name: str, ext: str) -> None:
        pass


class BZ2(Compressor):
    def write(self, file_name: int, data: iter, ext: str = "bz2"):
        with bz2.open("{}/{}.{}".format(self.path, file_name, ext), "wb") as comp_file:
            for row in data:
                comp_file.write(row)
