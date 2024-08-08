from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest
from dapla import FileClient

from fagfunksjoner.paths.versions import next_version_number


@pytest.fixture
def mock_file_system():
    # Create a mock file system
    mock_fs = MagicMock()
    return mock_fs


@patch.object(FileClient, "get_gcs_file_system")
@patch("fagfunksjoner.paths.versions.check_env")
def test_get_next_version_number(
    mock_check_env, mock_get_gcs_file_system: FileClient, mock_file_system: Callable
):
    # Configure the mock to return the mock file system
    mock_check_env.return_value = "DAPLA"
    mock_get_gcs_file_system.return_value = mock_file_system

    # Test cases
    test_cases = [
        {
            "filepath": "bucket/data/2023/data_file_v1.parquet",
            "files": [
                "bucket/data/2023/data_file.parquet",
                "bucket/data/2023/data_file_v1.parquet",
                "bucket/data/2023/data_file_v3.parquet",
            ],
            "expected": 4,
        },
        {
            "filepath": "bucket/data/2023/data_file_v1.parquet",
            "files": [
                "bucket/data/2023/data_file.parquet"
                "bucket/data/2023/data_file_v1.parquet"
            ],
            "expected": 2,
        },
        {
            "filepath": "bucket/data/2023/data_file_v1.parquet",
            "files": [],
            "expected": 1,
        },
    ]

    for case in test_cases:
        mock_file_system.glob.return_value = case["files"]
        path: str = case["filepath"]
        result = next_version_number(path)
        assert (
            result == case["expected"]
        ), f"Expected {case['expected']} but got {result}"
