import pytest
from unittest.mock import Mock

@pytest.fixture(autouse=True)
def mock_google_client(mocker):
    mocker.patch('google.cloud.storage.Client', new=Mock())