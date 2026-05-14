import asyncio
from pipeline.sources import CSVSource, JSONLSource
from pipeline.sinks import JSONLSink, CSVSink
from pipeline.pipeline import Pipeline

async def main():
    #sink = JSONLSink("output.jsonl")

    sink = CSVSink("csv_sink_output.csv", ["age", "name"])

    pipeline = Pipeline(sink)
    pipeline.add_source(CSVSource("test.csv"))
    pipeline.add_source(JSONLSource("test_json.jsonl"))

    @pipeline.transform(["age"], "clean")
    def age_to_int(line):
        line["age"] = int(line["age"])
        return line
    
    @pipeline.transform(["age"], "transform")
    def age_to_months(line):
        line["age"] = line["age"]*12
        return line

    stats = await pipeline.run()
    print(f"stats: {stats}")

asyncio.run(main())