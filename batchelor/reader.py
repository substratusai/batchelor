import asyncio
import json

from google.cloud import storage
from smart_open import open


def parse_bucket(path: str) -> str:
    """
    Parse bucket name from a GCS path. For example given the path
    gs://bucket-name/path/to/file, return bucket-name
    """
    return path.split("/")[2]


def convert_path_to_list(path: str) -> list[str]:
    if path.startswith("gs://"):
        client = storage.Client()
        bucket_name = parse_bucket(path)
        paths = []
        for blob in client.list_blobs(bucket_name, prefix=path):
            paths.append(f"gs://{bucket_name}/{blob.name}")
        return paths
    return [path]


async def read_file_and_enqueue(path, queue: asyncio.Queue):
    paths = convert_path_to_list(path)
    for path in paths:
        with open(path, mode="r") as file:
            print(f"Sending request to Queue from file {path}")
            for line in file.readlines():
                request = json.loads(line)
                await queue.put(request)
    await queue.put(None)
