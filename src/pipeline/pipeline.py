import asyncio
from functools import wraps
from dataclasses import dataclass
from typing import Callable
import time

@dataclass
class TransformMetadata:
    func: Callable
    input_fields: list
    kind: str
    name:str

@dataclass
class PipelineStats:
    records_ingested_per_source: dict
    records_dropped_per_transform: dict
    records_written: dict
    errors: int
    walltime: float

class Pipeline:
    def __init__(self, sink):
        self.sink = sink
        self.sources = []
        self.transforms: list[TransformMetadata] = [] #list of transformations

        self.records_ingested_per_source: dict = {}
        self.records_dropped_per_transform: dict = {}
        self.records_written: int = 0
        self.errors = 0
        self.walltime = None
    
    def add_source(self, source):
        self.sources.append(source)
    
    async def run(self):
        #ingest from ALL sources CONCURRENTLY.
        queue = asyncio.Queue(maxsize = 20) #what should the maxsize be set to?

        start = time.perf_counter()

        producer_tasks = asyncio.create_task(self._produce(queue))
        consumer_tasks = asyncio.create_task(self._consume(queue))
        
        await producer_tasks
        await consumer_tasks

        end = time.perf_counter()
        self.walltime = end- start

        self.sink.close()


        ###CAN PUT CHECKS HERE: like check the numbers match up in PipelineStats

        #this isn't going anywhere lol. need to print it
        return PipelineStats(
            self.records_ingested_per_source,
            self.records_dropped_per_transform,
            self.records_written,
            self.errors,
            self.walltime
        )

    async def _consume(self, queue):
        while True:
            line = await queue.get() #get data from q
            if line is None: #if i add more, can track the number of Nones
                break #None sentinel signals finished.

            ##apply transformations.
            #you have a line = a dict.
    
            if self.transforms == []:
                self.sink.write(line)
                self.records_written += 1
            else:
                for trans in self.transforms:
                    function = trans.func
                    
                    #validate inpute fields before applying trans:
                    for field in trans.input_fields.keys():
                        val = line[field]
                        if val is None: #field does not exist
                            ## f"Input field {field} is missing"
                            # potentially drop the trecord
                        else:
                            if type(val) in trans.input_fields[field]:
                                break
                            else: #type does not match
                                # f"Input type for field {field} must be {trans.input_fields[field]}"

                    line = function(line) #apply transformation
                    print(f"transformation applied {trans.name}")
                    if line is None: #if it returns None, it means the record was dropped 
                        self.records_dropped_per_transform[trans.name] = self.records_dropped_per_transform.get(trans.name,0) + 1
                        print(f"record dropped {line}")
                        break
                self.sink.write(line)
                print(f"line written")
                self.records_written += 1
    
    async def _produce(self, queue):
        #you can use a taskgroup
        async with asyncio.TaskGroup() as tg:
            for source in self.sources: #this creates a producer, since each source is a producer?
                tg.create_task(source.stream(queue)) #wait for source to put all items in queue
                print(f"Producer created with source:{source.name}")
        for source in self.sources:
            self.records_ingested_per_source[source.name] = source.records_ingested
        await queue.put(None)

    def transform(self, input_fields: dict[str, list[type]], kind: str):
        def decorator(func):
            #wraps function. before it runs it, it appends info to self.transforms
            self.transforms.append(TransformMetadata(
                func = func,
                input_fields=input_fields,
                kind=kind,
                name=func.__name__,
            ))
            print(f"Transformation recorded: func: {func}, input_fields ={input_fields},kind = {kind}")
            return func
        return decorator 
