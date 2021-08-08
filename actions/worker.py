import bz2
import gzip
import json
import lzma
import os
import zlib
import psutil

from actions.reader import CSV
from actions.compressor import BZ2
from logger.logger import Logging

COMPRESSION_TYPES = {"BZ2": BZ2}
DATA_TYPES = {"DICT": dict, "STRING": str, "INTEGER": int, "BOOL": bool, "FLOAT": float, "LIST": list}
READERS = {"CSV": CSV}

METADATA_FILE_NAME = "metadata.json"
ENCODING = "utf-8"
PARTITION_BYTE = 100_000_000

logger = Logging(name=__name__).get_logger()


class Worker:
    def __init__(self, path):
        self.path = path
        self.metadata_dict = {}

        self.dataset_dict = self.create_dataset_dict()

    @staticmethod
    def get_compression_type(compression_type) -> object:
        return COMPRESSION_TYPES.get(compression_type)

    @staticmethod
    def get_reader_type(reader_type) -> object:
        return READERS.get(reader_type)

    @staticmethod
    def is_correct_metadata(metadata_dict: dict) -> bool:
        metadata_checker = True

        reader_class = metadata_dict.get("class")
        compressor_type = metadata_dict.get("compression")
        path = metadata_dict.get("path")
        schema = metadata_dict.get("schema")

        if reader_class:

            if not READERS.get(reader_class):
                metadata_checker = False
                logger.error("this class - {} isn't implemented".format(reader_class))

        else:
            metadata_checker = False
            logger.error("specify reader class")

        if compressor_type:

            if not COMPRESSION_TYPES.get(compressor_type):
                metadata_checker = False
                logger.error("this compression - {} isn't implemented".format(compressor_type))

        else:
            metadata_checker = False
            logger.error("specify compression type")

        if not path:
            metadata_checker = False
            logger.error("specify path to the dataset")

        if not schema:
            metadata_checker = False
            logger.error("specify schema to the dataset")

        return metadata_checker

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

    def _get_metadata_dict(self, dataset: str) -> dict:
        dataset_path = self.dataset_dict.get(dataset)

        if dataset_path:
            with open("{}/{}".format(dataset_path, METADATA_FILE_NAME), "r", encoding=ENCODING) as file:
                return json.load(file)

    def _create_metadata_file(self, dataset: str, metadata_dict: dict) -> None:

        if metadata_dict:

            with open("{}/{}/{}".format(self.path, dataset, METADATA_FILE_NAME), "w", encoding=ENCODING) as file:
                json.dump(metadata_dict, file)
        else:
            logger.error("metadata for {} isn't found".format(dataset))

    def init_dataset(self, dataset: str, metadata_dict: dict) -> None:

        if not self.is_exist_dataset(dataset):
            dataset_path = "{}/{}".format(self.path, dataset)
            os.mkdir(dataset_path)
            self.dataset_dict[dataset] = dataset_path

            self._create_metadata_file(dataset, metadata_dict)

    def compress_dataset(self, dataset: str):

        metadata_dict = self._get_metadata_dict(dataset)
        metadata_checker = self.is_correct_metadata(metadata_dict)

        sum_by_row = 0
        total_sum = 0
        sum_by_key = 0

        print(psutil.virtual_memory())

        if metadata_checker:

            path = metadata_dict.get("path")
            schema = metadata_dict.get("schema")
            partition = metadata_dict.get("partition_key")

            reader_obj = self.get_reader_type(reader_type=metadata_dict.get("class"))(path)
            compression_obj = self.get_compression_type(compression_type=metadata_dict.get("compression"))

            if not partition:
                for row in reader_obj.read():
                    print(row)
