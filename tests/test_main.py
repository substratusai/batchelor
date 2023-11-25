import asyncio
import pathlib
import os

import aiohttp
import pytest
from pytest_httpserver import HTTPServer

from batchelor.main import (
    flusher,
    worker,
    main,
    remove_ignored_fields,
    parse_ignore_fields,
)


@pytest.mark.asyncio
async def test_batching_and_flushing(tmp_path: pathlib.Path):
    results = asyncio.Queue(maxsize=100)
    flush_every = 10
    test_data = [{"title": f"result_{i}"} for i in range(25)]
    for data in test_data:
        await results.put(data)
    await results.put(None)  # Signal to end the loop

    await flusher(results, flush_every, output_path=str(tmp_path))
    assert len(os.listdir(tmp_path)) == 3
    # Ensure the total lines of the 3 files in tmp_path is equal to 25
    total_lines = 0
    for file in os.listdir(tmp_path):
        with open(tmp_path / file, mode="r") as f:
            print(f"Contents of file {file}: {f.read()}")
            f.seek(0)
            total_lines += len(f.readlines())

    assert total_lines == 25, f"Expected 25 lines, got {total_lines}."


@pytest.mark.asyncio
async def test_termination_condition():
    results = asyncio.Queue(maxsize=100)
    await results.put(None)  # Signal to end the loop
    await flusher(results, flush_every=10, output_path="")


@pytest.mark.asyncio
async def test_worker(httpserver: HTTPServer):
    dummy_data = {"foo": "bar"}
    dummy_request = {"title": "result_1"}
    httpserver.expect_request("/v1/completions").respond_with_json(dummy_data)
    url = httpserver.url_for("/v1/completions")
    requests = asyncio.Queue(maxsize=100)
    results = asyncio.Queue(maxsize=100)
    worker_id = 1
    session = aiohttp.ClientSession()
    await requests.put(dummy_request)
    await requests.put(None)
    task = asyncio.create_task(worker(requests, results, session, worker_id, url, []))
    await asyncio.wait_for(requests.join(), timeout=10)
    assert results.qsize() == 1
    result = await results.get()
    assert result == {"request": dummy_request, "response": dummy_data}
    await session.close()


@pytest.mark.asyncio
async def test_worker_ignore_fields(httpserver: HTTPServer):
    dummy_data = {"foo": "bar"}
    dummy_request = {"title": "result_1", "id": 1}
    ignore_fields = ["id"]
    expected_request = {"title": "result_1"}
    httpserver.expect_request(
        "/v1/completions", method="POST", json=expected_request
    ).respond_with_json(dummy_data)
    url = httpserver.url_for("/v1/completions")
    requests = asyncio.Queue(maxsize=100)
    results = asyncio.Queue(maxsize=100)
    worker_id = 1
    session = aiohttp.ClientSession()
    await requests.put(dummy_request)
    await requests.put(None)
    task = asyncio.create_task(
        worker(requests, results, session, worker_id, url, ignore_fields)
    )
    await asyncio.wait_for(requests.join(), timeout=10)
    assert results.qsize() == 1
    result = await results.get()
    assert result == {"request": dummy_request, "response": dummy_data}
    await session.close()


def test_remove_ignored_fields():
    request = {"title": "result_1", "id": 1}
    ignore_fields = ["id"]
    expected_request = {"title": "result_1"}
    assert remove_ignored_fields(request, ignore_fields) == expected_request


def test_remove_ignored_fields_empty_ignore_fields():
    request = {"title": "result_1", "id": 1}
    ignore_fields = []
    expected_request = {"title": "result_1", "id": 1}
    assert remove_ignored_fields(request, ignore_fields) == expected_request


def test_parse_ignore_fields():
    ignore_fields = "id,title"
    assert parse_ignore_fields(ignore_fields) == ["id", "title"]


def test_parse_ignore_fields_empty():
    ignore_fields = ""
    assert parse_ignore_fields(ignore_fields) == []


def test_parse_ignore_fields_single():
    assert parse_ignore_fields("id") == ["id"]
    assert parse_ignore_fields("id,  ") == ["id"]
    assert parse_ignore_fields(" id,  ") == ["id"]


def test_parse_ignore_fields_spaces():
    ignore_fields = " id , title  ,  "
    assert parse_ignore_fields(ignore_fields) == ["id", "title"]


@pytest.mark.asyncio
async def test_main(httpserver: HTTPServer, tmp_path: pathlib.Path):
    httpserver.expect_request("/v1/completions").respond_with_json({"foo": "bar"})
    url = httpserver.url_for("/v1/completions")
    requests_path = "tests/example_data.jsonl"
    output_path = str(tmp_path)
    flush_every = 90
    concurrency = 10
    ignore_fields = []
    await asyncio.wait_for(
        asyncio.create_task(
            main(
                requests_path, output_path, concurrency, flush_every, url, ignore_fields
            )
        ),
        timeout=10,
    )
    assert len(os.listdir(tmp_path)) == 3
