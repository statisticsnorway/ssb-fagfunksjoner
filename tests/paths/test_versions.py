from unittest.mock import patch

from fagfunksjoner.paths.versions import (
    get_fileversions,
    get_latest_fileversions,
    latest_version_number,
    latest_version_path,
    next_version_number,
    next_version_path,
)


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
    assert sorted(get_latest_fileversions(paths)) == sorted(expected)


# Test for latest_version_path function gs
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
def test_latest_version_path_gs(mock_get_fileversions, mock_latest_version_path):
    mock_get_fileversions.return_value = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
    ]
    mock_latest_version_path.return_value = "gs://bucket/folder/file_v2.parquet"
    filepath = "gs://bucket/folder/file_v1.parquet"
    assert latest_version_path(filepath) == "gs://bucket/folder/file_v2.parquet"


# Test for latest_version_path function local
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
def test_latest_version_path_local(mock_get_fileversions, mock_latest_version_path):
    mock_get_fileversions.return_value = [
        "/local/folder/file_v1.parquet",
        "/local/folder/file_v2.parquet",
    ]
    mock_latest_version_path.return_value = "/local/folder/file_v2.parquet"
    filepath = "/local/folder/file_v1.parquet"
    assert latest_version_path(filepath) == "/local/folder/file_v2.parquet"


# Test for latest_version_number function gs
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
def test_latest_version_number_gs(mock_get_fileversions, mock_latest_version_path):
    mock_get_fileversions.return_value = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
    ]
    mock_latest_version_path.return_value = "gs://bucket/folder/file_v2.parquet"
    filepath = "gs://bucket/folder/file_v1.parquet"
    assert latest_version_number(filepath) == 2


# Test for latest_version_number function local
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
def test_latest_version_number_local(mock_get_fileversions, mock_latest_version_path):
    mock_get_fileversions.return_value = [
        "/local/folder/file_v1.parquet",
        "/local/folder/file_v2.parquet",
    ]
    mock_latest_version_path.return_value = "/local/folder/file_v2.parquet"
    filepath = "/local/folder/file_v1.parquet"
    assert latest_version_number(filepath) == 2


# Test for next_version_number function
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
def test_next_version_number(mock_get_fileversions, mock_latest_version_number):
    mock_get_fileversions.return_value = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
    ]
    mock_latest_version_number.return_value = 2
    filepath = "gs://bucket/folder/file_v2.parquet"
    assert next_version_number(filepath) == 3


# Test for next_version_path function
@patch("fagfunksjoner.paths.versions.next_version_number")
@patch("fagfunksjoner.paths.versions.latest_version_path")
@patch("fagfunksjoner.paths.versions.get_version_number")
def test_next_version_path(
    mock_next_version_number, mock_latest_version_path, mock_get_version_number
):
    mock_next_version_number.return_value = 3
    mock_latest_version_path.return_value = "gs://bucket/folder/file_v2.parquet"
    mock_get_version_number.return_value = 2
    file_path = "gs://bucket/folder/file_v1.parquet"
    expected = "gs://bucket/folder/file_v3.parquet"
    assert next_version_path(file_path) == expected


def test_several_startswith():
    inputs = [
        "gs://bucket/folder/nevner_verifisert_v1.parquet",
        "gs://bucket/folder/nevner_verifisert_v12.parquet",
        "gs://bucket/folder/nevner_verifisert_v2.parquet",
        "gs://bucket/folder/nevner_v3.parquet",
    ]
    expected = [
        "gs://bucket/folder/nevner_verifisert_v12.parquet",
        "gs://bucket/folder/nevner_v3.parquet",
    ]
    assert sorted(get_latest_fileversions(inputs)) == sorted(expected)


@patch("fagfunksjoner.paths.versions.FileClient.get_gcs_file_system")
def test_without_version(mock_get_gcs_file_system):
    file_list = [
        "gs://bucket/folder/nevner_v1.parquet",
        "gs://bucket/folder/nevner_v2.parquet",
    ]
    mock_get_gcs_file_system.return_value.glob.return_value = file_list
    inputs = "gs://bucket/folder/nevner"
    assert get_fileversions(inputs) == file_list
