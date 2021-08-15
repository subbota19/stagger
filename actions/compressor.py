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
    def __init__(self, path: str, file_name: str, ext: str):
        self.path = path
        self.file_name = file_name
        self.ext = ext

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def write(self, data: list) -> None:
        pass


class BZ2(Compressor):
    def __init__(self, path: str, file_name: str, ext: str = "bz2"):
        super().__init__(path, file_name, ext)
        self.compressor_obj = bz2.BZ2File("{}/{}.{}".format(self.path, self.file_name, self.ext), "wb")

    def close(self):
        self.compressor_obj.close()

    def write(self, data: object):
        self.compressor_obj.write(data)
