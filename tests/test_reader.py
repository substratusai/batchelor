import asyncio
from dataclasses import dataclass
from unittest.mock import Mock

from google.cloud import storage

import pytest


@pytest.fixture
def mock_client(mocker):
    return mocker.patch("batchelor.reader.client", autospec=True)


def test_parse_bucket(mock_client):
    from batchelor.reader import parse_bucket

    input = "gs://bucket-name/path/to/file"
    expected = "bucket-name"
    assert parse_bucket(input) == expected

    input = "gcss://bucket-name/path/to/file"
    expected = "bucket-name"
    assert parse_bucket(input) == expected


@dataclass
class Blob:
    name: str


def test_convert_path_to_list_single(mocker, mock_client):
    path = "gs://bucket-name/path/to/file.json"
    mock_client.list_blobs.return_value = [Blob(name="path/to/file.json")]
    from batchelor.reader import convert_path_to_list

    output = convert_path_to_list(path)
    assert len(output) == 1
    assert output[0] == path
    assert mock_client.list_blobs.call_count == 1


def test_convert_path_to_list_multiple(mocker, mock_client):
    path = "gs://bucket-name/path"
    mock_client.list_blobs.return_value = [
        Blob(name="path/file1.jsonl"),
        Blob(name="path/file2.jsonl"),
    ]

    from batchelor.reader import convert_path_to_list

    output = convert_path_to_list(path)
    assert len(output) == 2
    assert output[0] == path + "/file1.jsonl"
    assert output[1] == path + "/file2.jsonl"
    assert mock_client.list_blobs.call_count == 1
