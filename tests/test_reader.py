import asyncio
from dataclasses import dataclass

from batchelor.reader import parse_bucket, convert_path_to_list

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
    mocker.patch(
        "google.cloud.storage.Client.list_blobs",
        return_value=[Blob(name="path/to/file.json")],
    )
    output = convert_path_to_list(path)
    assert len(output) == 1
    assert output[0] == path


def test_convert_path_to_list_single(mocker):
    path = "gs://bucket-name/path"
    # Mock the gcs list blobs method
    mocker.patch(
        "google.cloud.storage.Client.list_blobs",
        return_value=[
            Blob(name="path/file1.jsonl"),
            Blob(name="path/file2.jsonl"),],
    )
    output = convert_path_to_list(path)
    assert len(output) == 2
    assert output[0] == path + "/file1.jsonl"
    assert output[1] == path + "/file2.jsonl"
