import os
from unittest.mock import patch

import pandas as pd

from fagfunksjoner.paths.write_versioned import (
    write_unversioned_and_versioned_parquet,
    write_versioned_parquet,
)


def _df():
    return pd.DataFrame({"a": [1, 2, 3]})


def _make_base_dir() -> str:
    # Use a plausible absolute-like base for each OS, but we don't touch disk (IO is mocked)
    return "C:\\data" if os.name == "nt" else "/data"


def _ver(path: str, n: int) -> str:
    # Inject version suffix before file extension
    return path.replace(".parquet", f"_v{n}.parquet")


@patch("fagfunksjoner.paths.write_versioned._write_parquet")
@patch("fagfunksjoner.paths.write_versioned._exists")
@patch("fagfunksjoner.paths.write_versioned.get_fileversions")
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_write_unversioned_when_nothing_exists(
    mock_get_versions_versions, mock_get_versions_write, mock_exists, mock_write
):
    # Patch both the place-of-use in write_versioned and the source in versions
    mock_get_versions_versions.return_value = []
    mock_get_versions_write.return_value = []
    mock_exists.return_value = False

    path = os.path.join(_make_base_dir(), "file.parquet")
    written = write_unversioned_and_versioned_parquet(_df(), path)

    mock_write.assert_called_once()
    assert written == [path]


@patch("fagfunksjoner.paths.write_versioned._copy")
@patch("fagfunksjoner.paths.write_versioned._write_parquet")
@patch("fagfunksjoner.paths.write_versioned._exists")
@patch("fagfunksjoner.paths.versions._path_exists")
@patch("fagfunksjoner.paths.write_versioned.get_fileversions")
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_migrate_unversioned_to_v1_and_write_v2(
    mock_get_versions_versions,
    mock_get_versions_write,
    mock_versions_exists,
    mock_exists,
    mock_write,
    mock_copy,
):
    # Only unversioned exists
    mock_get_versions_versions.return_value = []
    mock_get_versions_write.return_value = []
    mock_exists.return_value = True
    mock_versions_exists.return_value = True  # so next_version_number returns 2

    path = os.path.join(_make_base_dir(), "file.parquet")
    written = write_unversioned_and_versioned_parquet(_df(), path)

    # Expect write of next version (v2) and update unversioned
    assert any(str(p).endswith("_v2.parquet") for p in written)
    assert path in written
    assert mock_copy.call_count >= 2  # unversioned->v1 and v2->unversioned
    mock_write.assert_called()


@patch("fagfunksjoner.paths.write_versioned._copy")
@patch("fagfunksjoner.paths.write_versioned._write_parquet")
@patch("fagfunksjoner.paths.write_versioned._exists")
@patch("fagfunksjoner.paths.write_versioned.get_fileversions")
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_write_next_when_versioned_exists(
    mock_get_versions_versions,
    mock_get_versions_write,
    mock_exists,
    mock_write,
    mock_copy,
):
    path = os.path.join(_make_base_dir(), "file.parquet")
    mock_get_versions_versions.return_value = [_ver(path, 1)]
    mock_get_versions_write.return_value = [_ver(path, 1)]
    mock_exists.return_value = False
    written = write_unversioned_and_versioned_parquet(_df(), path)

    # Should write next version (v2) and copy to unversioned
    assert any(str(p).endswith("_v2.parquet") for p in written)
    assert path in written
    mock_write.assert_called()
    mock_copy.assert_called()


@patch("fagfunksjoner.paths.write_versioned._write_parquet")
@patch("fagfunksjoner.paths.write_versioned._exists")
@patch("fagfunksjoner.paths.versions._path_exists")
@patch("fagfunksjoner.paths.write_versioned.get_fileversions")
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_versioned_only_starts_at_v1(
    mock_get_versions_versions,
    mock_get_versions_write,
    mock_versions_exists,
    mock_exists,
    mock_write,
):
    mock_get_versions_versions.return_value = []
    mock_get_versions_write.return_value = []
    mock_exists.return_value = False
    mock_versions_exists.return_value = False

    path = os.path.join(_make_base_dir(), "file.parquet")
    written = write_versioned_parquet(_df(), path)

    # Should write v1 only
    assert written and str(written[0]).endswith("_v1.parquet")
    mock_write.assert_called_once()


@patch("fagfunksjoner.paths.write_versioned.logger.warning")
@patch("fagfunksjoner.paths.write_versioned._write_parquet")
@patch("fagfunksjoner.paths.write_versioned._rename")
@patch("fagfunksjoner.paths.write_versioned._exists")
@patch("fagfunksjoner.paths.versions._path_exists")
@patch("fagfunksjoner.paths.write_versioned.get_fileversions")
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_versioned_only_migrates_unversioned_then_writes(
    mock_get_versions_versions,
    mock_get_versions_write,
    mock_versions_exists,
    mock_exists,
    mock_rename,
    mock_write,
    mock_warn,
):
    mock_get_versions_versions.return_value = []
    mock_get_versions_write.return_value = []
    mock_exists.return_value = True
    mock_versions_exists.return_value = True

    path = os.path.join(_make_base_dir(), "file.parquet")
    written = write_versioned_parquet(_df(), path)

    mock_warn.assert_called()
    mock_rename.assert_called()  # unversioned -> v1
    assert any(str(p).endswith("_v2.parquet") for p in written)
    mock_write.assert_called()


@patch("fagfunksjoner.paths.write_versioned.logger.warning")
@patch("fagfunksjoner.paths.write_versioned._delete")
@patch("fagfunksjoner.paths.write_versioned._write_parquet")
@patch("fagfunksjoner.paths.write_versioned._exists")
@patch("fagfunksjoner.paths.write_versioned.get_fileversions")
@patch("fagfunksjoner.paths.versions.get_fileversions")
def test_versioned_only_deletes_unversioned_when_versioned_exists(
    mock_get_versions_versions,
    mock_get_versions_write,
    mock_exists,
    mock_write,
    mock_delete,
    mock_warn,
):
    path = os.path.join(_make_base_dir(), "file.parquet")
    mock_get_versions_versions.return_value = [_ver(path, 3)]
    mock_get_versions_write.return_value = [_ver(path, 3)]
    mock_exists.return_value = True
    written = write_versioned_parquet(_df(), path)

    mock_warn.assert_called()
    mock_delete.assert_called_once()
    assert any(str(p).endswith("_v4.parquet") for p in written)
    mock_write.assert_called()
