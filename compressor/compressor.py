from logger.logger import Logging

import zlib
import gzip
import bz2
import lzma
import os
import json

COMPRESSION_TYPES = {"zlib": zlib, "glib": gzip, "bz2": bz2, "lzma": lzma}
DATA_TYPES = {"DICT": dict, "STRING": str, "INTEGER": int, "BOOL": bool, "FLOAT": float, "LIST": list}
METADATA_FILE_NAME = "metadata.json"
ENCODING = "utf-8"

logger = Logging(name=__name__).get_logger()


class Compressor:
    def __init__(self, path, compression_type="bz2"):
        self.path = path
        self.metadata_dict = {}

        self.compression_type = self.get_compression_type(compression_type)
        self.dataset_dict = self.create_dataset_dict()

    @staticmethod
    def get_compression_type(compression_type) -> object:
        return COMPRESSION_TYPES.get(compression_type)

    def create_dataset_dict(self) -> dict:
        dataset_dict = {}

        for dataset in os.listdir(self.path):
            dataset_dict[dataset] = "{}/{}".format(self.path, dataset)

        return dataset_dict

    def is_exist_dataset(self, dataset: str) -> bool:
        is_exist = False

        if self.dataset_dict.get(dataset):
            is_exist = True

        return is_exist

    def add_metadata_dict(self, metadata_dict: dict, dataset: str) -> None:
        self.metadata_dict[dataset] = metadata_dict

    def create_metadata_file(self, dataset: str) -> None:

        metadata_dict = self.metadata_dict.get(dataset)

        if metadata_dict:

            with open("{}/{}/{}".format(self.path, dataset, METADATA_FILE_NAME), "w", encoding=ENCODING) as file:
                json.dump(metadata_dict, file)
        else:
            logger.error("metadata for {} isn't found".format(dataset))

    def init_dataset(self, dataset: str) -> None:

        if not self.is_exist_dataset(dataset):
            dataset_path = "{}/{}".format(self.path, dataset)
            os.mkdir(dataset_path)
            self.dataset_dict[dataset] = dataset_path

            self.create_metadata_file(dataset)

    def compress_dataset(self, dataset: str):
        pass
