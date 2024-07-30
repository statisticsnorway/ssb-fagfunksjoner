from collections.abc import Callable
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from dapla import FileClient
from fagfunksjoner.paths.versions import get_next_version_number


@pytest.fixture
def mock_file_system():
    # Create a mock file system
    mock_fs = MagicMock()
    return mock_fs


@patch.object(FileClient, "get_gcs_file_system")
def test_get_next_version_number(
    mock_get_gcs_file_system: FileClient, mock_file_system: Callable
):
    # Configure the mock to return the mock file system
    mock_get_gcs_file_system.return_value = mock_file_system

    # Test cases
    test_cases = [
        {
            "filepath": "bucket/data/2023/data_file.parquet",
            "files": [
                "bucket/data/2023/data_file.parquet",
                "bucket/data/2023/data_file_v1.parquet",
                "bucket/data/2023/data_file_v2.parquet",
                "bucket/data/2023/data_file_v3.parquet",
            ],
            "expected": 4,
        },
        {
            "filepath": "bucket/data/2023/data_file.parquet",
            "files": [
                "bucket/data/2023/data_file.parquet"
                "bucket/data/2023/data_file_v1.parquet"
            ],
            "expected": 2,
        },
        {"filepath": "bucket/data/2023/data_file.parquet", "files": [], "expected": 1},
    ]

    for case in test_cases:
        mock_file_system.ls.return_value = case["files"]
        path: str = case["filepath"]
        result = get_next_version_number(path)
        assert (
            result == case["expected"]
        ), f"Expected {case['expected']} but got {result}"
