import csv
import json
from pipeline.sinks import JSONLSink, CSVSink


def test_jsonl_sink_writes_records(tmp_path):
    path = str(tmp_path / "out.jsonl")
    sink = JSONLSink(path, headers=["name", "age"])
    sink.write({"name": "alice", "age": 30})
    sink.write({"name": "bob", "age": 25})
    sink.close()

    lines = (tmp_path / "out.jsonl").read_text().strip().splitlines()
    assert json.loads(lines[0]) == {"name": "alice", "age": 30}
    assert json.loads(lines[1]) == {"name": "bob", "age": 25}


def test_jsonl_sink_each_record_on_own_line(tmp_path):
    path = str(tmp_path / "out.jsonl")
    sink = JSONLSink(path, headers=["x"])
    sink.write({"x": 1})
    sink.write({"x": 2})
    sink.close()
    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    assert len(lines) == 2


def test_csv_sink_writes_records(tmp_path):
    path = str(tmp_path / "out.csv")
    sink = CSVSink(path, headers=["name", "age"])
    sink.write({"name": "alice", "age": 30})
    sink.write({"name": "bob", "age": 25})
    sink.close()

    with open(path) as f:
        rows = list(csv.DictReader(f))
    assert rows == [{"name": "alice", "age": "30"}, {"name": "bob", "age": "25"}]


def test_csv_sink_respects_header_order(tmp_path):
    path = str(tmp_path / "out.csv")
    sink = CSVSink(path, headers=["age", "name"])
    sink.write({"name": "alice", "age": 30})
    sink.close()

    with open(path) as f:
        header_line = f.readline().strip()
    assert header_line == "age,name"


def test_jsonl_sink_empty(tmp_path):
    path = str(tmp_path / "out.jsonl")
    sink = JSONLSink(path, headers=[])
    sink.close()
    assert (tmp_path / "out.jsonl").read_text() == ""
