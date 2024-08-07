from unittest.mock import patch

import pytest

from fagfunksjoner.paths.versions import (
    get_latest_fileversions,
    latest_version_number,
    next_version_number,
    next_version_path,
    split_path,
)


# Test for split_path function
def test_split_path_correct():
    filepath = "gs://bucket/folder/file_v1.parquet"
    expected = ("gs://bucket/folder/file_", "v1", ".parquet")
    assert split_path(filepath) == expected


def test_split_path_incorrect():
    filepath = "gs://bucket/folder/file_1.parquet"
    with pytest.raises(ValueError, match="Version not following standard"):
        split_path(filepath)


# Test for get_latest_fileversions function
def test_get_latest_fileversions():
    paths = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
        "gs://bucket/folder/otherfile_v1.parquet",
        "gs://bucket/folder/otherfile_v3.parquet",
    ]
    expected = [
        "gs://bucket/folder/file_v2.parquet",
        "gs://bucket/folder/otherfile_v3.parquet",
    ]
    assert get_latest_fileversions(paths) == expected


# Test for latest_version_number function
@patch("fagfunksjoner.paths.versions.FileClient.get_gcs_file_system")
@patch("fagfunksjoner.paths.versions.glob.glob")
@patch("fagfunksjoner.paths.versions.check_env")
def test_latest_version_number(mock_check_env, mock_glob, mock_get_gcs_file_system):
    mock_check_env.return_value = "DAPLA"
    mock_get_gcs_file_system.return_value.glob.return_value = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
    ]
    filepath = "gs://bucket/folder/file_v1.parquet"
    assert latest_version_number(filepath) == 2

    mock_check_env.return_value = "LOCAL"
    mock_glob.return_value = [
        "/local/folder/file_v1.parquet",
        "/local/folder/file_v2.parquet",
    ]
    filepath = "/local/folder/file_v1.parquet"
    assert latest_version_number(filepath) == 2


# Test for next_version_number function
@patch("fagfunksjoner.paths.versions.latest_version_number")
def test_next_version_number(mock_latest_version_number):
    mock_latest_version_number.return_value = 2
    filepath = "gs://bucket/folder/file_v2.parquet"
    assert next_version_number(filepath) == 3


# Test for next_version_path function
@patch("fagfunksjoner.paths.versions.next_version_number")
def test_next_version_path(mock_next_version_number):
    mock_next_version_number.return_value = 2
    filepath = "gs://bucket/folder/file_v1.parquet"
    expected = "gs://bucket/folder/file_v2.parquet"
    assert next_version_path(filepath) == expected
