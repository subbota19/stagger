import json
import os
import sys

import psutil

from actions.compressor import BZ2
from actions.reader import CSV
from logger.logger import Logging

COMPRESSION_TYPES = {"BZ2": BZ2}
DATA_TYPES = {"DICT": dict, "STRING": str, "INTEGER": int, "BOOL": bool, "FLOAT": float, "LIST": list}
READERS = {"CSV": CSV}

METADATA_FILE_NAME = "metadata.json"
DATA_DIR = "den"
ENCODING = "utf-8"
PARTITION_BYTE = 1_000_000_000
SEP = "_"
LINE_SEPARATOR = "\n"

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
    def _get_partition_dict(schema: dict, key_is_number=False) -> dict:

        if key_is_number:
            output_schema = {key: 0 for key in range(len(schema))}
        else:
            output_schema = {key: 0 for key in schema.keys()}

        return output_schema

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

            data_path = "{}/{}".format(dataset_path, DATA_DIR)
            os.mkdir(data_path)

            self._create_metadata_file(dataset, metadata_dict)

    def compress_dataset(self, dataset: str):

        metadata_dict = self._get_metadata_dict(dataset)
        metadata_checker = self.is_correct_metadata(metadata_dict)

        sum_by_row = 0
        total_sum = 0
        sum_by_key = 0

        print(psutil.virtual_memory())

        if metadata_checker:

            logger.info("metadata_checker was successfully checked")

            path = metadata_dict.get("path")
            schema = metadata_dict.get("schema")
            partition = metadata_dict.get("partition_key")
            data_path = "{}/{}/{}".format(self.path, dataset, DATA_DIR)

            partition_analyzer = self._get_partition_dict(schema=schema, key_is_number=True)
            partition_schema = {index: key for index, key in enumerate(schema, start=0)}
            partition_compressor_dict = {}

            reader_obj = self.get_reader_type(reader_type=metadata_dict.get("class"))(path)

            if not partition:
                partition_path = "{}/{}".format(data_path, partition)

                if not os.path.exists(partition_path):
                    os.mkdir(partition_path)

                counter, summator = 0, 0

                for row in reader_obj.read():

                    if summator >= PARTITION_BYTE:
                        for key in schema:
                            compressor_obj = partition_compressor_dict.get(key)
                            compressor_obj.close()

                        summator = 0

                    if summator == 0:
                        counter += 1

                        modified_partition_path = "{}/{}".format(partition_path, counter)

                        if not os.path.exists(modified_partition_path):
                            os.mkdir(modified_partition_path)

                        for key in schema:
                            partition_compressor_dict[key] = self.get_compression_type(
                                metadata_dict.get("compression"))(modified_partition_path, key)

                    for index, value in enumerate(row, start=0):
                        value_size = sys.getsizeof(value)
                        partition_analyzer[index] += value_size
                        summator += value_size

                        key = partition_schema.get(index)
                        compressor_obj = partition_compressor_dict.get(key)

                        compressor_obj.write(bytes(value + "\n", encoding=ENCODING))
