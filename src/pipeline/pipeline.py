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
    records_errored_per_transform: int
    walltime: float

class Pipeline:
    def __init__(self, sink, error_sink = None):
        self.sink = sink
        self.error_sink = error_sink
        self.sources = []
        self.transforms: list[TransformMetadata] = [] #list of transformations

        self.records_ingested_per_source: dict = {}
        self.records_dropped_per_transform: dict = {}
        self.records_errored_per_transform: dict = {}
        self.records_written: int = 0
        self.walltime = None
    
    def _validate_record(self, record, trans):
        """
        ok = apply
        skip = skip this transform
        error = something's broken
        """
        for field, expected_type in trans.input_fields.items():
            if field not in record:
                return "skip"
            if not isinstance(record[field], expected_type):
                return "error"
        return "ok"
    
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
            self.records_errored_per_transform,
            self.walltime
        )

    async def _consume(self, queue):
            while True:
                record = await queue.get()
                if record is None:
                    break

                drop = False
                for trans in self.transforms:
                    status = self._validate_record(record, trans)

                    if status == "skip":
                        continue

                    if status == "error":
                        self.records_errored_per_transform[trans.name] = \
                            self.records_errored_per_transform.get(trans.name, 0) + 1
                        if self.error_sink:
                            self.error_sink.write({"record": record, "transform": trans.name, "reason": "wrong type"})
                        drop = True
                        break

                    # status == "ok"
                    try:
                        record = trans.func(record)
                    except Exception as e:
                        self.records_errored_per_transform[trans.name] = \
                            self.records_errored_per_transform.get(trans.name, 0) + 1
                        if self.error_sink:
                            self.error_sink.write({"record": record, "transform": trans.name, "reason": str(e)})
                        drop = True
                        break

                    if record is None:
                        self.records_dropped_per_transform[trans.name] = \
                            self.records_dropped_per_transform.get(trans.name, 0) + 1
                        drop = True
                        break

                if drop:
                    continue
                self.sink.write(record)
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

    def transform(self, input_fields: dict = None, kind: str = ""):
        def decorator(func):
            #wraps function. before it runs it, it appends info to self.transforms
            self.transforms.append(TransformMetadata(
                func=func,
                input_fields=input_fields or {}, #empty dict DEFAULT
                kind=kind,
                name=func.__name__,
            ))
            print(f"Transformation recorded: func: {func}, input_fields ={input_fields},kind = {kind}")
            return func
        return decorator