from fnmatch import fnmatch

from dapla import FileClient

from fagfunksjoner.paths.versions import get_latest_gcs_files


def test_get_latest_gcs_files(monkeypatch, Mock_filesys):

    monkeypatch.setattr(FileClient, "get_gcs_file_system", Mock_filesys)

    latest_files = get_latest_gcs_files(
        "ssb-test-dapla-team-data-produkt/testdata",
        file_format="parquet",
        by='version'
        )
    print(latest_files)
    assert len(latest_files) == 2