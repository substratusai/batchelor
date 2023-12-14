from dataclasses import dataclass
import asyncio

from batchelor.reader import parse_bucket, convert_path_to_list

import pytest

def test_parse_bucket():
    input = "gs://bucket-name/path/to/file"
    expected = "bucket-name"
    assert parse_bucket(input) == expected

    input = "gcss://bucket-name/path/to/file"
    expected = "bucket-name"
    assert parse_bucket(input) == expected


def test_convert_path_to_list(mocker):
    @dataclass
    class Blob:
        name: str

    path = "gs://bucket-name/path/to/file.json"
    # Mock the gcs list blobs method
    mocker.patch(
        "google.cloud.storage.Client.list_blobs",
        return_value=[Blob(name="path/to/file.json")],
    )
    output = convert_path_to_list(path)
    assert len(output) == 1
    assert output[0] == path

