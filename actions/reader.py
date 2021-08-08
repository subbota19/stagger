from abc import ABC, abstractmethod
import csv
import sys
import os


class Reader(ABC):
    def __init__(self, path):
        self.path = path

    def get_file_size(self) -> int:
        if os.path.exists(self.path):
            return os.path.getsize(self.path)

    @abstractmethod
    def read(self):
        pass


class CSV(Reader):
    def read(self) -> list:
        with open(self.path, "r") as file:
            csv_reader = csv.reader(file, delimiter=',')

            for row in csv_reader:
                yield row
