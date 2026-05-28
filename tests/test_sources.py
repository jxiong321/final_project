import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from pipeline.sources import CSVSource, JSONLSource, HTTPSource


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("name,age\nalice,30\nbob,25\n")
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


async def drain(queue) -> list:
    records = []
    while not queue.empty():
        records.append(await queue.get())
    return records


async def test_csv_source_streams_all_rows(csv_file):
    source = CSVSource(csv_file)
    queue = asyncio.Queue()
    await source.stream(queue)
    records = await drain(queue)
    assert records == [{"name": "alice", "age": "30"}, {"name": "bob", "age": "25"}]


async def test_csv_source_tracks_ingested_count(csv_file):
    source = CSVSource(csv_file)
    queue = asyncio.Queue()
    await source.stream(queue)
    assert source.records_ingested == 2


async def test_csv_source_name_is_path(csv_file):
    source = CSVSource(csv_file)
    assert source.name == csv_file


async def test_jsonl_source_streams_all_rows(jsonl_file):
    source = JSONLSource(jsonl_file)
    queue = asyncio.Queue()
    await source.stream(queue)
    records = await drain(queue)
    assert records == [{"name": "alice", "score": 10}, {"name": "bob", "score": 20}]


async def test_jsonl_source_tracks_ingested_count(jsonl_file):
    source = JSONLSource(jsonl_file)
    queue = asyncio.Queue()
    await source.stream(queue)
    assert source.records_ingested == 2


async def test_csv_source_empty_file(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("name,age\n")
    source = CSVSource(str(path))
    queue = asyncio.Queue()
    await source.stream(queue)
    assert source.records_ingested == 0
    assert queue.empty()


async def test_jsonl_source_empty_file(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_text("")
    source = JSONLSource(str(path))
    queue = asyncio.Queue()
    await source.stream(queue)
    assert source.records_ingested == 0
    assert queue.empty()


# HTTPSource (mocked)
def make_mock_session(json_data: object) -> MagicMock:
    mock_response = AsyncMock()
    mock_response.json = AsyncMock(return_value=json_data)
    #session.get(url) is an async context manager
    mock_get_cm = AsyncMock()
    mock_get_cm.__aenter__ = AsyncMock(return_value=mock_response)
    mock_get_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_get_cm)

    #ClientSession() is also an async context manager
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)
    return mock_session_cm


async def test_http_source_streams_records():
    fake_data = [{"name": "alice", "score": 10}, {"name": "bob", "score": 20}]

    with patch("pipeline.sources.aiohttp.ClientSession", return_value=make_mock_session(fake_data)):
        source = HTTPSource("https://example.com/data.json", parse_fn=lambda data: data)
        queue: asyncio.Queue = asyncio.Queue()
        await source.stream(queue)

    records = await drain(queue)
    assert records == fake_data


async def test_http_source_tracks_ingested_count():
    fake_data = [{"id": 1}, {"id": 2}, {"id": 3}]

    with patch("pipeline.sources.aiohttp.ClientSession", return_value=make_mock_session(fake_data)):
        source = HTTPSource("https://example.com/data.json", parse_fn=lambda data: data)
        queue: asyncio.Queue = asyncio.Queue()
        await source.stream(queue)

    assert source.records_ingested == 3


async def test_http_source_uses_parse_fn():
    fake_response = {"results": [{"name": "alice"}, {"name": "bob"}]}

    with patch("pipeline.sources.aiohttp.ClientSession", return_value=make_mock_session(fake_response)):
        source = HTTPSource(
            "https://example.com/api",
            parse_fn=lambda data: data["results"],
        )
        queue: asyncio.Queue = asyncio.Queue()
        await source.stream(queue)

    records = await drain(queue)
    assert records == [{"name": "alice"}, {"name": "bob"}]


async def test_http_source_name_is_url():
    fake_data: list = []

    with patch("pipeline.sources.aiohttp.ClientSession", return_value=make_mock_session(fake_data)):
        source = HTTPSource("https://example.com/data.json", parse_fn=lambda data: data)
        assert source.name == "https://example.com/data.json"


async def test_http_source_empty_response():
    with patch("pipeline.sources.aiohttp.ClientSession", return_value=make_mock_session([])):
        source = HTTPSource("https://example.com/empty", parse_fn=lambda data: data)
        queue: asyncio.Queue = asyncio.Queue()
        await source.stream(queue)

    assert source.records_ingested == 0
    assert queue.empty()
