from actions.worker import Worker
from parser.parser import Parser
import os
import time
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

    worker_obj = Worker(path=path_data)

    for dataset, metadata in metadata_dict.items():
        t1 = time.time()
        worker_obj.init_dataset(dataset=dataset, metadata_dict=metadata)
        worker_obj.compress_dataset(dataset=dataset)

        print(time.time() - t1)


if __name__ == "__main__":
    main()
