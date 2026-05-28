import asyncio
from pipeline.sources import CSVSource, JSONLSource
from pipeline.sinks import CSVSink
from pipeline.pipeline import Pipeline

async def main():
    sink = CSVSink("csv_sink_output.csv", ["age", "name"])
    pipeline = Pipeline(sink)
    pipeline.add_source(CSVSource("test.csv"))
    pipeline.add_source(JSONLSource("test_json.jsonl"))

    @pipeline.transform(input_fields={"age": int}, kind="clean")
    def age_to_int(line):
        line["age"] = int(line["age"])
        return line

    #skipped if a record has no "tier" field
    @pipeline.transform(input_fields={"tier": str}, kind="clean")
    def tag_premium(line):
        line["tier"] = "premium" if line["age"] > 360 else "standard"
        return line

    #drop anyone under 18
    @pipeline.transform(input_fields={"age": int}, kind="filter")
    def drop_minors(line):
        if line["age"] < 18:
            return None
        return line

    report = await pipeline.dry_run()
    pipeline.print_dry_run(report)

    stats = await pipeline.run()
    stats.print()

asyncio.run(main())
