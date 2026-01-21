from pathlib import Path
from unittest.mock import patch

from fagfunksjoner.paths.versions import (
    construct_file_pattern,
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


def test_get_latest_fileversions_local():
    paths = [
        "/bucket/folder/file_v1__DOC.json",
        "/bucket/folder/file_v2__DOC.json",
        "/bucket/folder/file_v2.parquet",
        "/bucket/folder/otherfile_v10.parquet",
        "/bucket/folder/otherfile_v3.parquet",
    ]
    expected = [
        "/bucket/folder/file_v2.parquet",
        "/bucket/folder/otherfile_v10.parquet",
        "/bucket/folder/file_v2__DOC.json",
    ]
    assert sorted(get_latest_fileversions(paths)) == sorted(expected)


def test_construct_file_pattern():
    file_path = "/bucket/folder/file_v102__DOC.json"
    expected = "/bucket/folder/file_v*__DOC.json"
    assert construct_file_pattern(file_path) == expected

    file_path = "/bucket/folder/file_v1.parquet"
    expected = "/bucket/folder/file_v*.parquet"
    assert construct_file_pattern(file_path) == expected

    file_path = "/bucket/folder/file_very_v1.parquet"
    expected = "/bucket/folder/file_very_v*.parquet"
    assert construct_file_pattern(file_path) == expected


@patch("fagfunksjoner.paths.versions.glob.glob")
@patch("fagfunksjoner.paths.versions.construct_file_pattern")
def test_get_fileversions(mock_construct_file_pattern, mock_glob):
    # Mock the return value of construct_file_pattern
    mock_construct_file_pattern.return_value = "/local/folder/file_v*__DOC.json"

    # Mock the return value of glob.glob
    mock_glob.return_value = [
        "/local/folder/file_v1__DOC.json",
        "/local/folder/file_v2__DOC.json",
    ]

    # Call the function with a sample input
    filepath = "/local/folder/file_v1__DOC.json"
    result = get_fileversions(filepath)

    # Assertions
    mock_construct_file_pattern.assert_called_once_with(filepath)
    mock_glob.assert_called_once_with("/local/folder/file_v*__DOC.json")
    assert sorted(result) == sorted(
        [
            "/local/folder/file_v1__DOC.json",
            "/local/folder/file_v2__DOC.json",
        ]
    )


# Test for `latest_version_path` function with Google Storage path
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_latest_version_path_gs(mock_get_fileversions):
    mock_get_fileversions.return_value = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
    ]
    filepath = "gs://bucket/folder/file_v1.parquet"
    assert latest_version_path(filepath) == "gs://bucket/folder/file_v2.parquet"


# Test for `latest_version_path` function with local path
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_latest_version_path_local(mock_get_fileversions):
    mock_get_fileversions.return_value = [
        "/local/folder/file_v1.parquet",
        "/local/folder/file_v2.parquet",
    ]
    filepath = "/local/folder/file_v1.parquet"
    assert latest_version_path(filepath) == "/local/folder/file_v2.parquet"


# Test for `latest_version_path` function with local path
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_latest_version_path_local_doc_json(mock_get_fileversions):
    mock_get_fileversions.return_value = [
        "/local/folder/file_v1__DOC.json",
        "/local/folder/file_v2__DOC.json",
    ]
    filepath = "/local/folder/file_v1__DOC.json"
    assert latest_version_path(filepath) == "/local/folder/file_v2__DOC.json"


# Test for `latest_version_path` function with local path
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_latest_version_path_local_path(mock_get_fileversions):
    mock_get_fileversions.return_value = [
        Path("/local/folder/file_v1.parquet"),
        Path("/local/folder/file_v2.parquet"),
    ]
    filepath = Path("/local/folder/file_v1.parquet")
    assert isinstance(latest_version_path(filepath), Path)
    assert latest_version_path(filepath) == Path("/local/folder/file_v2.parquet")


# Test for `latest_version_number` function with Google Storage path
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
def test_latest_version_number_gs(mock_latest_version_path, mock_get_fileversions):
    mock_get_fileversions.return_value = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
    ]
    mock_latest_version_path.return_value = "gs://bucket/folder/file_v2.parquet"
    filepath = "gs://bucket/folder/file_v1.parquet"
    assert latest_version_number(filepath) == 2


# Test for `next_version_number` function with Google Storage path
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
def test_next_version_number(mock_latest_version_path, mock_get_fileversions):
    mock_get_fileversions.return_value = ["gs://bucket/folder/file_v2.parquet"]
    mock_latest_version_path.return_value = "gs://bucket/folder/file_v2.parquet"
    filepath = "gs://bucket/folder/file_v2.parquet"
    assert next_version_number(filepath) == 3


# Test for `latest_version_path` function to check if it defaults to '_v1'
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.construct_file_pattern")
def test_latest_version_path_defaults_to_v1(
    mock_construct_file_pattern, mock_get_fileversions
):
    mock_get_fileversions.return_value = []
    mock_construct_file_pattern.return_value = "gs://bucket/folder/file_v1.parquet"
    filepath = "gs://bucket/folder/file.parquet"
    assert latest_version_path(filepath) == "gs://bucket/folder/file_v1.parquet"


# Test for `next_version_path` function with Google Storage path
@patch("fagfunksjoner.paths.versions.get_fileversions")
@patch("fagfunksjoner.paths.versions.latest_version_path")
@patch("fagfunksjoner.paths.versions.get_version_number")
def test_next_version_path(
    mock_get_version_number, mock_latest_version_path, mock_get_fileversions
):
    mock_get_fileversions.return_value = [
        "gs://bucket/folder/file_v1.parquet",
        "gs://bucket/folder/file_v2.parquet",
    ]
    mock_latest_version_path.return_value = "gs://bucket/folder/file_v2.parquet"
    mock_get_version_number.return_value = 2

    file_path = "gs://bucket/folder/file_v2.parquet"
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
