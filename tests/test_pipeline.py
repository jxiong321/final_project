import json
import pytest
from pipeline.pipeline import Pipeline, PipelineStats
from pipeline.sources import CSVSource, JSONLSource

#helpers
class MemorySink:
    def __init__(self):
        self.records: list = []
        self.closed = False

    def write(self, record):
        self.records.append(record)

    def close(self):
        self.closed = True


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("name,score\nalice,10\nbob,20\ncharlie,30\n")
    return str(path)

@pytest.fixture
def jsonl_file(tmp_path):
    path = tmp_path / "data.jsonl"
    lines = [
        json.dumps({"name": "alice", "score": 10}),
        json.dumps({"name": "bob", "score": 20}),
    ]
    path.write_text("\n".join(lines) + "\n")
    return str(path)


#Basic pipeline executio
async def test_pipeline_writes_records_without_transforms(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))
    await p.run()
    assert len(sink.records) == 3
    assert sink.closed


async def test_pipeline_applies_transform(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))

    @p.transform(input_fields={"name": str})
    def upper_name(record):
        record["name"] = record["name"].upper()
        return record

    await p.run()
    names = [r["name"] for r in sink.records]
    assert names == ["ALICE", "BOB", "CHARLIE"]


async def test_pipeline_chained_transforms(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))

    @p.transform(input_fields={"name": str})
    def upper_name(record):
        record["name"] = record["name"].upper()
        return record

    @p.transform(input_fields={"name": str})
    def add_greeting(record):
        record["greeting"] = f"Hello {record['name']}"
        return record

    await p.run()
    assert sink.records[0]["greeting"] == "Hello ALICE"


async def test_pipeline_returns_stats(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))
    stats = await p.run()
    assert isinstance(stats, PipelineStats)
    assert stats.records_written == 3
    assert stats.walltime > 0


#Record dropping (transform returns None)
async def test_transform_returning_none_drops_record(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))

    @p.transform(input_fields={"name": str})
    def drop_alice(record):
        if record["name"] == "alice":
            return None
        return record

    stats = await p.run()
    assert len(sink.records) == 2
    assert stats.records_dropped_per_transform["drop_alice"] == 1


#Skip (missing field)
async def test_transform_skipped_when_field_missing(tmp_path):
    path = tmp_path / "data.jsonl"
    path.write_text(json.dumps({"name": "alice"}) + "\n" + json.dumps({"name": "bob", "score": 5}) + "\n")

    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(JSONLSource(str(path)))

    @p.transform(input_fields={"score": int})
    def double_score(record):
        record["score"] = record["score"] * 2
        return record

    await p.run()
    #alice has no score so transform is skipped, record still written
    names = [r["name"] for r in sink.records]
    assert "alice" in names
    #bob's score should be doubled
    bob = next(r for r in sink.records if r["name"] == "bob")
    assert bob["score"] == 10


#Error handling
async def test_transform_exception_sends_to_error_sink(csv_file):
    sink = MemorySink()
    error_sink = MemorySink()
    p = Pipeline(sink=sink, error_sink=error_sink)
    p.add_source(CSVSource(csv_file))

    @p.transform(input_fields={"name": str})
    def always_crash(record):
        raise ValueError("boom")

    await p.run()
    assert len(sink.records) == 0
    assert len(error_sink.records) == 3
    assert error_sink.records[0]["reason"] == "boom"
    assert error_sink.records[0]["transform"] == "always_crash"


async def test_transform_exception_counted_in_stats(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))

    @p.transform(input_fields={"name": str})
    def always_crash(record):
        raise RuntimeError("oops")

    stats = await p.run()
    assert stats.records_errored_per_transform["always_crash"] == 3


async def test_error_without_error_sink_still_drops_record(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))

    @p.transform(input_fields={"name": str})
    def always_crash(record):
        raise RuntimeError("oops")

    await p.run()
    assert len(sink.records) == 0


# Multiple sources
async def test_multiple_sources_all_ingested(csv_file, jsonl_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))
    p.add_source(JSONLSource(jsonl_file))
    stats = await p.run()
    # csv has 3 rows, jsonl has 2
    assert stats.records_written == 5


async def test_ingested_per_source_tracked(csv_file, jsonl_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    csv_source = CSVSource(csv_file)
    jsonl_source = JSONLSource(jsonl_file)
    p.add_source(csv_source)
    p.add_source(jsonl_source)
    stats = await p.run()
    assert stats.records_ingested_per_source[csv_source.name] == 3
    assert stats.records_ingested_per_source[jsonl_source.name] == 2


#Dry run
async def test_dry_run_reports_clean(jsonl_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(JSONLSource(jsonl_file))

    @p.transform(input_fields={"name": str})
    def upper_name(record):
        record["name"] = record["name"].upper()
        return record

    report = await p.dry_run()
    assert any("clean" in line.lower() for line in report)


async def test_dry_run_reports_missing_field(jsonl_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(JSONLSource(jsonl_file))

    @p.transform(input_fields={"nonexistent_field": str})
    def needs_missing(record):
        return record

    report = await p.dry_run()
    assert any("skip" in line.lower() for line in report)


async def test_dry_run_reports_type_mismatch(tmp_path):
    path = tmp_path / "data.jsonl"
    path.write_text(json.dumps({"score": "not_an_int"}) + "\n")

    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(JSONLSource(str(path)))

    @p.transform(input_fields={"score": int})
    def double(record):
        record["score"] *= 2
        return record

    report = await p.dry_run()
    assert any("type" in line.lower() for line in report)


async def test_dry_run_writes_nothing(csv_file):
    sink = MemorySink()
    p = Pipeline(sink=sink)
    p.add_source(CSVSource(csv_file))

    @p.transform(input_fields={"name": str})
    def upper_name(record):
        record["name"] = record["name"].upper()
        return record

    await p.dry_run()
    assert len(sink.records) == 0
