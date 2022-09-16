import csv
from . import MediaData


def to_csv(generator, filename):
    with open(filename, "w", newline="") as csvfile:
        fieldnames = list(MediaData.__required_keys__)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in generator:
            writer.writerow(data)
