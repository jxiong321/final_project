import csv
import json
from abc import ABC, abstractmethod
import asyncio
import aiohttp

class Source(ABC):
    @abstractmethod
    async def stream(self, queue):
        pass

class CSVSource(Source):
    def __init__(self, path):
        self.path = path
        self.name = self.path
        self.records_ingested = 0
        #should track fieldnames for CSVSinks
    
    async def stream(self, queue):
        #open file
        with open(self.path, mode='r') as file:
            reader = csv.DictReader(file)
    
            for row in reader:
                #in future: try/except errors
                await queue.put(row)
                self.records_ingested += 1
                print(f"Source {self.name} records ingested ={self.records_ingested}")

        #close the file when done: auto closes

class JSONLSource(Source):
    def __init__(self, path):
        self.path = path
        self.name = self.path
        self.records_ingested = 0
    
    async def stream(self, queue):
        #open file
        with open(self.path, mode='r') as file:
            reader = (json.loads(line) for line in file)

            field_names = set()

            for row in reader:
                await queue.put(row)
                self.records_ingested += 1
                print(f"Source {self.name} records ingested ={self.records_ingested}")

                field_names.update(row.keys())
            

class HTTPSource(Source):
    def __init__(self, url, parse_fn):
        self.url = url
        self.name = self.url
        self.parse_fn = parse_fn
        self.records_ingested = 0
    
    #FIGURE OUT HOW TO MOCK HTTPSOURCE FOR TESTS
    async def stream(self, queue):
        async with aiohttp.ClientSession() as session: #start a new session
            async with session.get(self.url) as response:
                data = await response.json()
                records = self.parse_fn(data) #returns LIST of records 
                for record in records:
                    await queue.put(record)
                    self.records_ingested += 1


if __name__ == "__main__":

    async def main():
        queue = asyncio.Queue()
        source = CSVSource("test.csv")
        await source.stream(queue)
        # drain the queue and print
        while not queue.empty():
            print(queue.get_nowait())

    asyncio.run(main())