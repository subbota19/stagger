from actions.worker import Worker
from parser.parser import Parser
import os
import argparse


def args_parser() -> object:
    parser = argparse.ArgumentParser()

    parser.add_argument('--ds', dest='datasets')

    return parser.parse_args()


def main():
    os.environ.get("STAGGER_DATA")
    path_data = "/home/yauheni/PyCharmProjects/stagger/data"
    path_meta = "/home/yauheni/PyCharmProjects/stagger/metadata"

    args = args_parser()

    parser_obj = Parser(dataset_str=args.datasets, path=path_meta)
    metadata_dict = parser_obj.get_metadata()

    compressor_obj = Worker(path=path_data)

    for dataset, metadata in metadata_dict.items():
        compressor_obj.init_dataset(dataset=dataset, metadata_dict=metadata)
        compressor_obj.compress_dataset(dataset=dataset)


if __name__ == "__main__":
    main()
