from unittest.mock import Mock, patch

from fagfunksjoner.paths.versions import get_latest_gcs_files


@patch("fagfunksjoner.paths.versions.FileClient")
def test_get_latest_gcs_files(file_client_mock: Mock, mock_filesys):
    file_client_mock.get_gcs_file_system.return_value = mock_filesys

    latest_files = get_latest_gcs_files(
        "ssb-test-dapla-team-data-produkt/testdata",
        file_format="parquet",
        by='version'
        )
    print(latest_files)
    assert len(latest_files) == 2