from pathlib import Path

import pytest

from fagfunksjoner.paths.shared_files import (
    FileState,
    check_shared_files,
)


def make_file(
    directory: Path,
    name: str,
) -> Path:
    file_path = directory / name
    file_path.touch()
    return file_path


def test_ready_file(tmp_path: Path) -> None:
    make_file(
        tmp_path,
        "observasjoner-mnd_p2025_v1.parquet",
    )

    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
            "description": "Observasjoner per måned",
        }
    ]

    report = check_shared_files(files, year=2025)

    status = report.files[0]

    assert status.state is FileState.READY
    assert status.version == 1
    assert status.file_name == "observasjoner-mnd_p2025_v1.parquet"
    assert status.is_ready is True


def test_latest_version_is_selected(tmp_path: Path) -> None:
    make_file(
        tmp_path,
        "observasjoner-mnd_p2025_v0.parquet",
    )
    make_file(
        tmp_path,
        "observasjoner-mnd_p2025_v1.parquet",
    )
    make_file(
        tmp_path,
        "observasjoner-mnd_p2025_v3.parquet",
    )

    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    status = report.files[0]

    assert status.state is FileState.READY
    assert status.version == 3
    assert status.file_name == "observasjoner-mnd_p2025_v3.parquet"


def test_version_zero_is_draft(tmp_path: Path) -> None:
    make_file(
        tmp_path,
        "observasjoner-mnd_p2025_v0.parquet",
    )

    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    status = report.files[0]

    assert status.state is FileState.DRAFT
    assert status.version == 0
    assert status.is_ready is False


def test_missing_file(tmp_path: Path) -> None:
    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    status = report.files[0]

    assert status.state is FileState.MISSING
    assert status.version is None
    assert status.file_path is None
    assert status.is_ready is False


def test_wrong_year_is_missing(tmp_path: Path) -> None:
    make_file(
        tmp_path,
        "observasjoner-mnd_p2024_v2.parquet",
    )

    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    assert report.files[0].state is FileState.MISSING


def test_description_is_optional(tmp_path: Path) -> None:
    make_file(
        tmp_path,
        "observasjoner-mnd_p2025_v1.parquet",
    )

    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    assert report.files[0].description == ""


@pytest.mark.parametrize(
    "files, expected_key",
    [
        (
            [
                {
                    "path": "/tmp",
                }
            ],
            "name",
        ),
        (
            [
                {
                    "name": "observasjoner-mnd",
                }
            ],
            "path",
        ),
    ],
)
def test_missing_required_key(
    files,
    expected_key: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=expected_key,
    ):
        check_shared_files(files, year=2025)


def test_unexpected_key_raises_error(tmp_path: Path) -> None:
    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
            "test": "skrivefeil",
        }
    ]

    with pytest.raises(
        ValueError,
        match="Unexpected key",
    ):
        check_shared_files(files, year=2025)


def test_report_groups_files_by_state(tmp_path: Path) -> None:
    make_file(
        tmp_path,
        "ready_p2025_v2.parquet",
    )
    make_file(
        tmp_path,
        "draft_p2025_v0.parquet",
    )

    files = [
        {
            "name": "ready",
            "path": tmp_path,
        },
        {
            "name": "draft",
            "path": tmp_path,
        },
        {
            "name": "missing",
            "path": tmp_path,
        },
    ]

    report = check_shared_files(files, year=2025)

    assert len(report.ready) == 1
    assert len(report.draft) == 1
    assert len(report.missing) == 1
    assert report.all_ready is False


def test_all_ready(tmp_path: Path) -> None:
    make_file(tmp_path, "first_p2025_v1.parquet")
    make_file(tmp_path, "second_p2025_v3.parquet")

    files = [
        {
            "name": "first",
            "path": tmp_path,
        },
        {
            "name": "second",
            "path": tmp_path,
        },
    ]

    report = check_shared_files(files, year=2025)

    assert report.all_ready is True


def test_report_output(tmp_path: Path) -> None:
    make_file(
        tmp_path,
        "observasjoner-mnd_p2025_v2.parquet",
    )

    files = [
        {
            "name": "observasjoner-mnd",
            "path": tmp_path,
            "description": "Observasjoner per måned",
        }
    ]

    report = check_shared_files(files, year=2025)

    output = str(report)

    assert "Name" in output
    assert "Description" in output
    assert "Version" in output
    assert "Modified at" in output
    assert "observasjoner-mnd_p2025_v2.parquet" in output
    assert "Observasjoner per måned" in output
    assert "v2" in output
    assert "🟢" in output


def test_latest_version_with_two_digits(tmp_path: Path) -> None:
    make_file(tmp_path, "observasjoner_p2025_v2.parquet")
    make_file(tmp_path, "observasjoner_p2025_v10.parquet")

    files = [
        {
            "name": "observasjoner",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    assert report.files[0].version == 10
    assert report.files[0].file_name == "observasjoner_p2025_v10.parquet"


def test_non_parquet_files_are_ignored(tmp_path: Path) -> None:
    make_file(tmp_path, "observasjoner_p2025_v1.parquet")
    make_file(tmp_path, "observasjoner_p2025_v2.json")

    files = [
        {
            "name": "observasjoner",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    assert report.files[0].version == 1
    assert report.files[0].file_path.suffix == ".parquet"


def test_invalid_version_filename_is_ignored(tmp_path: Path) -> None:
    make_file(tmp_path, "observasjoner_p2025_v2.parquet")
    make_file(tmp_path, "observasjoner_p2025_vfinal.parquet")

    files = [
        {
            "name": "observasjoner",
            "path": tmp_path,
        }
    ]

    report = check_shared_files(files, year=2025)

    assert report.files[0].version == 2
