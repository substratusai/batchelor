import asyncio
from dataclasses import dataclass
from unittest.mock import Mock

from google.cloud import storage
import pytest

from batchelor.reader import parse_bucket, convert_path_to_list


@pytest.fixture
def mock_client(mocker):
    return mocker.patch("google.cloud.storage.Client", autospec=True)


def test_parse_bucket(mock_client):
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
    mock_client.return_value.list_blobs.return_value = [Blob(name="path/to/file.json")]

    output = convert_path_to_list(path)
    assert mock_client.return_value.list_blobs.call_count == 1
    assert len(output) == 1
    assert output[0] == path


def test_convert_path_to_list_multiple(mocker, mock_client):
    path = "gs://bucket-name/path"
    mock_client.return_value.list_blobs.return_value = [
        Blob(name="path/file1.jsonl"),
        Blob(name="path/file2.jsonl"),
        Blob(name="path/file3.json"),
        Blob(name="path/file4"),
    ]

    output = convert_path_to_list(path)
    assert mock_client.return_value.list_blobs.call_count == 1
    assert len(output) == 3
    assert output[0] == path + "/file1.jsonl"
    assert output[1] == path + "/file2.jsonl"
    assert output[2] == path + "/file3.json"
