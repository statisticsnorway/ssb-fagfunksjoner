from unittest.mock import Mock, patch

from fsspec.implementations.local import LocalFileSystem

from fagfunksjoner.paths.version import get_latest_gcs_files


@patch("fagfunksjoner.paths.version.FileClient")
def test_get_latest_gcs_files(file_client_mock: Mock, testdata_get_latest_gcs_files):
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock.glob.return_value = testdata_get_latest_gcs_files

    latest_files = get_latest_gcs_files("gcs-bucket/path/testdir")
    
    assert len(latest_files) == 2