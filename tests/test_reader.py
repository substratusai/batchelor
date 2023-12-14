import asyncio
from dataclasses import dataclass
from unittest.mock import Mock

from batchelor.reader import parse_bucket, convert_path_to_list
from google.cloud import storage

import pytest


def test_parse_bucket():
    input = "gs://bucket-name/path/to/file"
    expected = "bucket-name"
    assert parse_bucket(input) == expected

    input = "gcss://bucket-name/path/to/file"
    expected = "bucket-name"
    assert parse_bucket(input) == expected


@dataclass
class Blob:
    name: str


def test_convert_path_to_list_single(mocker):
    path = "gs://bucket-name/path/to/file.json"
    # Mock the gcs list blobs method
    mock_client = mocker.patch("batchelor.reader.client")
    mock_client.list_blobs.return_value = [Blob(name="path/to/file.json")]

    output = convert_path_to_list(path)
    assert len(output) == 1
    assert output[0] == path
    assert mock_client.list_blobs.call_count == 1


def test_convert_path_to_list_multiple(mocker):
    path = "gs://bucket-name/path"
    # Mock the gcs list blobs method
    mock_client = mocker.patch("batchelor.reader.client")
    mock_client.list_blobs.return_value = [
        Blob(name="path/file1.jsonl"),
        Blob(name="path/file2.jsonl"),
    ]
    output = convert_path_to_list(path)
    assert len(output) == 2
    assert output[0] == path + "/file1.jsonl"
    assert output[1] == path + "/file2.jsonl"
    assert mock_client.list_blobs.call_count == 1
