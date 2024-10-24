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
def test_get_next_version_number(
    mock_get_gcs_file_system: FileClient, mock_file_system: Callable
):
    mock_get_gcs_file_system.return_value = mock_file_system

    # Test cases
    test_cases = [
        {
            "filepath": "ssb-bucket/data/2023/data_file_v1.parquet",
            "files": [
                "ssb-bucket/data/2023/data_file.parquet",
                "ssb-bucket/data/2023/data_file_v1.parquet",
                "ssb-bucket/data/2023/data_file_v3.parquet",
            ],
            "expected": 4,
        },
        {
            "filepath": "gs://bucket/data/2023/data_file_v1.parquet",
            "files": [
                "gs://bucket/data/2023/data_file.parquet"
                "gs://bucket/data/2023/data_file_v1.parquet"
            ],
            "expected": 2,
        },
        {
            "filepath": "http://bucket/data/2023/data_file_v1.parquet",
            "files": [],
            "expected": 0,
        },
    ]

    for case in test_cases:
        mock_file_system.glob.return_value = case["files"]
        path: str = case["filepath"]

        # Mocking input if no files are found
        if not case["files"]:
            with patch("builtins.input", return_value=str(case["expected"])):
                result = next_version_number(path)
        else:
            result = next_version_number(path)
        assert (
            result == case["expected"]
        ), f"Expected {case['expected']} but got {result}"
