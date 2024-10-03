from fnmatch import fnmatch

from dapla import FileClient

from fagfunksjoner.paths.versions import get_latest_gcs_files
from .conftest import testdata_get_latest_gcs_files


class MockFS:

    @staticmethod
    def glob(globstr: str, detail: bool = False) -> list | dict:
        data = testdata_get_latest_gcs_files()

        files = []

        for file in data.keys():
            if fnmatch(file, globstr):
                files.append(file)
        
        if detail:
            relevant_files = {}
            for file in files:
                relevant_files[file] = data.get(file)
        
        else:
            relevant_files = [f for f in files]
        
        return relevant_files


def test_get_latest_gcs_files(monkeypatch, testdata_get_latest_gcs_files):

    def mock_get_fs(*args, **kwargs):
        return MockFS()

    # apply the monkeypatch for requests.get to mock_get
    monkeypatch.setattr(FileClient, "get_gcs_file_system", mock_get_fs)

    latest_files = get_latest_gcs_files(
        "ssb-test-dapla-team-data-produkt/testdata",
        file_format="parquet",
        by='version'
        )
    print(latest_files)
    assert len(latest_files) == 2