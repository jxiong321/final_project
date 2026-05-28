import json
import csv
from typing import Any
from io import TextIOWrapper

Record = dict[str, Any]


class JSONLSink:
    def __init__(self, path: str, headers: list[str]) -> None:
        self.path = path
        self.headers = headers
        self.file: TextIOWrapper = open(path, "w")

    def write(self, record: Record) -> None:
        # if headers specified, only write those keys
        out = {k: record[k] for k in self.headers if k in record} if self.headers else record
        self.file.write(json.dumps(out) + "\n")

    def close(self) -> None:
        self.file.close()


class CSVSink:
    def __init__(self, path: str, headers: list[str]) -> None:
        self.path = path
        self.headers = headers
        self.file: TextIOWrapper = open(path, "w")
        self.writer = csv.DictWriter(self.file, headers, extrasaction="ignore")
        self.writer.writeheader()

    def write(self, record: Record) -> None:
        self.writer.writerow(record)

    def close(self) -> None:
        self.file.close()
