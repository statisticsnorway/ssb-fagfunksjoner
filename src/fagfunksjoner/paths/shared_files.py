from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from ..fagfunksjoner_logger import silence_logger
from .git import repo_root_dir
from .versions import get_latest_fileversions, get_version_number


REQUIRED_FILE_KEYS = {"name", "path"}
OPTIONAL_FILE_KEYS = {"description"}
ALLOWED_FILE_KEYS = REQUIRED_FILE_KEYS | OPTIONAL_FILE_KEYS

class FileState(StrEnum):
    """Status for a shared file."""

    READY = "READY"
    DRAFT = "DRAFT"
    MISSING = "MISSING"


@dataclass(frozen=True)
class SharedFileSpec:
    """Specification of a shared file dependency."""

    name: str
    path: Path
    description: str | None = None


@dataclass(frozen=True)
class FileStatus:
    """Status information for one shared file."""

    spec: SharedFileSpec
    year: int
    state: FileState
    version: int | None = None
    file_path: Path | None = None
    modified_at: datetime | None = None
    warnings: tuple[str, ...] = ()

    @property
    def is_ready(self) -> bool:
        """Return whether the file is ready for use."""
        return self.state is FileState.READY

    @property
    def file_name(self) -> str:
        """Return actual filename or expected filename pattern."""
        if self.file_path is not None:
            return self.file_path.name

        return f"{self.spec.name}_p{self.year}_v*.parquet"

    @property
    def description(self) -> str:
        """Return description or an empty string."""
        return self.spec.description or ""


@dataclass
class FileStatusReport:
    """Status report for a collection of shared files."""

    year: int
    files: list[FileStatus] = field(default_factory=list)

    @property
    def ready(self) -> list[FileStatus]:
        """Return files with version 1 or higher."""
        return [
            file
            for file in self.files
            if file.state is FileState.READY
        ]

    @property
    def draft(self) -> list[FileStatus]:
        """Return files with version 0."""
        return [
            file
            for file in self.files
            if file.state is FileState.DRAFT
        ]

    @property
    def missing(self) -> list[FileStatus]:
        """Return files not found for the requested year."""
        return [
            file
            for file in self.files
            if file.state is FileState.MISSING
        ]

    @property
    def all_ready(self) -> bool:
        """Return whether all configured files are ready."""
        return bool(self.files) and all(
            file.is_ready for file in self.files
        )

    def __repr__(self) -> str:
        return self._format_report()

    def __str__(self) -> str:
        return self._format_report()

    def _format_report(self) -> str:
        """Create a human-readable status report."""
        icons = {
            FileState.READY: "🟢",
            FileState.DRAFT: "🟡",
            FileState.MISSING: "🔴",
        }

        name_header = "Name"
        description_header = "Description"
        version_header = "Version"
        modified_header = "Modified at"

        name_width = max(
            [len(name_header)]
            + [len(file.file_name) for file in self.files]
        )

        description_width = max(
            [len(description_header)]
            + [len(file.description) for file in self.files]
        )

        version_width = len(version_header)

        lines = [
            f"Filstatus for {self.year}",
            "",
            (
                f"   "
                f"{name_header:<{name_width}}  "
                f"{description_header:<{description_width}}  "
                f"{version_header:<{version_width}}  "
                f"{modified_header}"
            ),
        ]

        for file in self.files:
            icon = icons[file.state]

            version = (
                f"v{file.version}"
                if file.version is not None
                else "–"
            )

            modified = (
                file.modified_at.strftime("%Y-%m-%d %H:%M")
                if file.modified_at is not None
                else "Ikke funnet"
            )

            lines.append(
                f"{icon} "
                f"{file.file_name:<{name_width}}  "
                f"{file.description:<{description_width}}  "
                f"{version:<{version_width}}  "
                f"{modified}"
            )

        lines.extend(
            [
                "",
                f"{len(self.ready)} av {len(self.files)} filer er klare",
            ]
        )

        warnings = [
            warning
            for file in self.files
            for warning in file.warnings
        ]

        if warnings:
            lines.extend(["", "Advarsler:"])
            lines.extend(
                f"⚠️ {warning}"
                for warning in warnings
            )

        return "\n".join(lines)


def _resolve_path(path: str | Path) -> Path:
    """Resolve relative paths from the repository root."""
    path = Path(path)

    if path.is_absolute():
        return path

    return repo_root_dir() / path


def _parse_file_specs(
    files: Sequence[Mapping[str, Any]],
) -> list[SharedFileSpec]:
    """Convert dict-like configuration to file specifications.

    Each file specification must contain the keys ``name`` and ``path``.
    The optional key ``description`` may also be provided.
    """
    specs: list[SharedFileSpec] = []

    for config in files:
        keys = set(config)

        missing_keys = REQUIRED_FILE_KEYS - keys
        if missing_keys:
            raise ValueError(
                "Missing required key(s) in shared file configuration: "
                f"{', '.join(sorted(missing_keys))}. "
                "Expected keys are 'name', 'path' and optionally "
                "'description'."
            )

        unexpected_keys = keys - ALLOWED_FILE_KEYS
        if unexpected_keys:
            raise ValueError(
                "Unexpected key(s) in shared file configuration: "
                f"{', '.join(sorted(unexpected_keys))}. "
                "Expected keys are 'name', 'path' and optionally "
                "'description'."
            )

        specs.append(
            SharedFileSpec(
                name=config["name"],
                path=_resolve_path(config["path"]),
                description=config.get("description"),
            )
        )

    return specs


def _check_file(
    spec: SharedFileSpec,
    year: int,
) -> FileStatus:
    """Check availability and latest version of one shared file."""
    pattern = f"{spec.name}_p{year}_v*.parquet"

    candidates = list(spec.path.glob(pattern))

    if not candidates:
        return FileStatus(
            spec=spec,
            year=year,
            state=FileState.MISSING,
        )

    latest_files = silence_logger(
        get_latest_fileversions,
        candidates,
    )

    if not latest_files:
        return FileStatus(
            spec=spec,
            year=year,
            state=FileState.MISSING,
            warnings=(
                f"Fant filer for '{spec.name}', "
                "men kunne ikke bestemme siste versjon.",
            ),
        )

    latest_file = Path(latest_files[0])
    version = get_version_number(latest_file)

    state = (
        FileState.DRAFT
        if version == 0
        else FileState.READY
    )

    modified_at = datetime.fromtimestamp(
        latest_file.stat().st_mtime
    )

    warnings: list[str] = []

    if len(latest_files) > 1:
        warnings.append(
            f"Fant flere siste filversjoner for "
            f"'{spec.name}': "
            + ", ".join(
                Path(file).name
                for file in latest_files
            )
        )

    return FileStatus(
        spec=spec,
        year=year,
        state=state,
        version=version,
        file_path=latest_file,
        modified_at=modified_at,
        warnings=tuple(warnings),
    )


def check_shared_files(
    files: Sequence[Mapping[str, Any]],
    year: int,
) -> FileStatusReport:
    """Check shared input files for a given year.

    Files are expected to follow the SSB naming convention:

        <name>_p<year>_v<version>.parquet

    Version 0 is interpreted as draft data and gets yellow status.
    Version 1 or higher is interpreted as ready data and gets green
    status. If no file is found for the requested year, the file gets
    red status.

    The modification time is read from the mounted filesystem using
    ``Path.stat().st_mtime``. For mounted Google Cloud Storage buckets,
    this represents the modification time exposed by Cloud Storage FUSE,
    not the GCS object's ``timeCreated`` metadata.

    Absolute paths, such as paths below ``/buckets``, are used directly.
    Relative paths are resolved from the root of the current Git
    repository using ``repo_root_dir``.

    Args:
        files:
            Sequence of dict-like file specifications. Each item must
            contain ``name`` and ``path``. ``description`` is optional.

            Example:

                [
                    {
                        "name": "observasjoner-mnd",
                        "path": "/buckets/shared/data",
                        "description": "Observasjoner per måned",
                    },
                    {
                        "name": "observasjoner-imputert",
                        "path": "/buckets/shared/data",
                        "description": "Imputerte observasjoner",
                    },
                ]

            The input can for example come directly from Dynaconf using
            ``settings.shared_files``.

        year:
            Reference year to check, for example ``2025``.

    Returns:
        FileStatusReport containing status, latest version, filename and
        modification time for each configured file.
    """
    specs = _parse_file_specs(files)

    statuses = [
        _check_file(spec, year)
        for spec in specs
    ]

    return FileStatusReport(
        year=year,
        files=statuses,
    )