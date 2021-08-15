import bz2

path = "/home/yauheni/PyCharmProjects/stagger/data/test/den/None/1/Country.bz2"

with bz2.open(path, "rt") as f:
    for row in f.readlines():
        print(repr(row.rstrip("\n")))
