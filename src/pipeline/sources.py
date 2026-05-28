import csv
import json
from abc import ABC, abstractmethod
import asyncio
from typing import Callable

import aiohttp

Record = dict[str, object]


class Source(ABC):
    name: str
    records_ingested: int

    @abstractmethod
    async def stream(self, queue: asyncio.Queue[Record | None]) -> None:
        pass


class CSVSource(Source):
    """a CSV Source"""
    def __init__(self, path: str) -> None:
        self.path = path
        self.name = path
        self.records_ingested = 0

    async def stream(self, queue: asyncio.Queue[Record | None]) -> None:
        with open(self.path, mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                await queue.put(dict(row))
                self.records_ingested += 1


class JSONLSource(Source):
    """ A JSONL source """
    def __init__(self, path: str) -> None:
        self.path = path
        self.name = path
        self.records_ingested = 0

    async def stream(self, queue: asyncio.Queue[Record | None]) -> None:
        with open(self.path, mode="r") as file:
            for line in file:
                row: Record = json.loads(line)
                await queue.put(row)
                self.records_ingested += 1


class HTTPSource(Source):
    """ An HTTP Source"""
    def __init__(self, url: str, parse_fn: Callable[[object], list[Record]]) -> None:
        self.url = url
        self.name = url
        self.parse_fn = parse_fn
        self.records_ingested = 0

    async def stream(self, queue: asyncio.Queue[Record | None]) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                data = await response.json()
                records = self.parse_fn(data)
                for record in records:
                    await queue.put(record)
                    self.records_ingested += 1
