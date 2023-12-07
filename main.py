"""
Batch inference using asyncio.Queue
"""

import argparse
import asyncio
import json
import os
import traceback

import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from smart_open import open

url = "http://localhost:8080/v1/completions"
filename = "part-{partition}.jsonl"
ignore_fields = []
concurrency = 100
requests_path = ""
output_path = "/tmp/lingo-batch-inference"
flush_every = 1000
timeout = 1200


async def read_file_and_enqueue(path, queue: asyncio.Queue):
    with open(path, mode="r") as file:
        print(f"Sending request to Queue from file {path}")
        for line in file.readlines():
            request = json.loads(line)
            await queue.put(request)
    await queue.put(None)


async def worker(
    requests: asyncio.Queue,
    results: asyncio.Queue,
    session: aiohttp.ClientSession,
    worker_id: int,
    url: str,
    ignore_fields: list[str] = [],
):
    print(f"Starting worker {worker_id}")
    while True:
        request = await requests.get()
        print(f"{worker_id}: Got request {request}")
        if request is None:
            requests.task_done()
            break
        try:
            parsed_request = remove_ignored_fields(request, ignore_fields)
            async with session.post(url=url, json=parsed_request) as response:
                print(f"{worker_id}: HTTP {response.status}")
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}: {await response.text()}")
                response = await response.json()
                await results.put({"request": request, "response": response})
                requests.task_done()
        except Exception as e:
            await results.put({"request": request, "error": str(e)})
            requests.task_done()
            traceback.print_exc()


async def flusher(results: asyncio.Queue, flush_every: int, output_path: str):
    print("Starting flusher")
    current_batch = []
    partition = 1
    while True:
        result = await results.get()
        if result is None and len(current_batch) == 0:
            break
        current_batch.append(result)
        if len(current_batch) >= flush_every or result is None:
            if result is None:
                current_batch.pop()
                print(f"Flushing last batch of {len(current_batch) - 1} results")
            else:
                print(f"Flushing batch of {len(current_batch)} results")
            jsonl_data = "\n".join(json.dumps(entry) for entry in current_batch)
            partitioned_filename = (
                output_path + "/" + filename.format(partition=partition)
            )
            with open(partitioned_filename, mode="w") as file:
                file.write(jsonl_data)
            current_batch = []
            partition += 1
            if result is None:
                results.task_done()
                break


async def main(
    requests_path, output_path, concurrency, flush_every, url, ignore_fields, timeout
):
    requests = asyncio.Queue(maxsize=concurrency)
    results = asyncio.Queue()
    timeout = aiohttp.ClientTimeout(total=timeout)
    conn = aiohttp.TCPConnector(limit=0)
    session = aiohttp.ClientSession(timeout=timeout, connector=conn)
    retry_options = ExponentialRetry(attempts=3, statuses={500, 502, 503, 504})
    retry_client = RetryClient(client_session=session, retry_options=retry_options)
    producer_task = asyncio.create_task(read_file_and_enqueue(requests_path, requests))
    flusher_task = asyncio.create_task(flusher(results, flush_every, output_path))
    workers = [
        asyncio.create_task(
            worker(requests, results, retry_client, worker_id, url, ignore_fields)
        )
        for worker_id in range(concurrency)
    ]
    await producer_task
    print("Finished reading requests file and enqueueing requests")
    print("Waiting for all requests to be processed")
    await requests.join()
    print("All requests have been processed")
    await session.close()
    # Send a signal that all requests have been processed
    await results.put(None)
    print("Waiting for all results to be flushed")
    await flusher_task
    for w in workers:
        w.cancel()


def parse_ignore_fields(ignore_fields: str) -> list[str]:
    """
    Ignore fields in the request. Example: --ignore-fields 'id,bar'

    @param ignore_fields: Comma-separated list of fields to ignore

    Returns a list of the ignored fields.
    """
    if ignore_fields == "":
        return []
    fields = [field.strip() for field in ignore_fields.split(",")]
    fields = list(filter(lambda field: field != "", fields))
    return fields


def remove_ignored_fields(request: dict, ignore_fields: list[str]) -> dict:
    """
    Remove ignored fields from the request.

    @param request: Request dict
    @param ignore_fields: List of fields to ignore

    Returns a new request dict without the ignored fields.
    """
    return {key: value for key, value in request.items() if key not in ignore_fields}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Lingo using Python OpenAI API")
    parser.add_argument("--url", type=str, default=os.environ.get("URL", url))
    parser.add_argument(
        "--requests-path",
        type=str,
        default=os.environ.get("REQUESTS_PATH", requests_path),
        help="Path to read requests file",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=os.environ.get("OUTPUT_PATH", output_path),
        help="Path to write output files",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=os.environ.get("FLUSH_EVERY", flush_every),
        help="Number of requests to flush to disk",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=os.environ.get("CONCURRENCY", concurrency),
        help=f"Number of concurrent requests. Defaults to {concurrency}",
    )
    parser.add_argument(
        "--ignore-fields",
        type=str,
        default=os.environ.get("IGNORE_FIELDS", ""),
        help="Fields to ignore in the request. Example: --ignore-fields 'id,bar'",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=os.environ.get("TIMEOUT", timeout),
        help=f"Request timeout in seconds. Defaults to {timeout}",
    )
    args = parser.parse_args()
    requests_path = args.requests_path
    output_path = args.output_path
    flush_every = args.flush_every
    concurrency = args.concurrency
    ignore_fields = parse_ignore_fields(args.ignore_fields)
    url = args.url
    timeout = args.timeout
    asyncio.run(
        main(
            requests_path,
            output_path,
            concurrency,
            flush_every,
            url,
            ignore_fields,
            timeout,
        )
    )
