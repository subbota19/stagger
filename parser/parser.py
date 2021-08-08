from logger.logger import Logging

import os
import json

ENCODING = "utf-8"

logger = Logging(name=__name__).get_logger()


class Parser:
    def __init__(self, dataset_str: str, path: str):
        self.path = path

        self.dataset_list = self.split_dataset_str(dataset_str)

    @staticmethod
    def split_dataset_str(dataset_str: str, splitter: str = "|") -> list:
        return dataset_str.split(splitter)

    def get_metadata(self, ext: str = "json"):
        metadata_dict = {}

        for dataset in self.dataset_list:
            dataset_path = "{}/{}.{}".format(self.path, dataset, ext)

            if os.path.exists(dataset_path):

                with open(dataset_path, "r", encoding=ENCODING) as file:
                    metadata_dict[dataset] = json.load(file)
            else:
                logger.error("metafile by path - {} isn't exist".format(dataset_path))

        return metadata_dict
