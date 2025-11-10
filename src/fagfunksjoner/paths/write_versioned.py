"""Utilities for writing DataFrames to versioned and unversioned paths.

This module performs file-handling operations (local or GCS) while
delegating all path/version logic to `fagfunksjoner.paths.versions`.

Opinionated behavior for Parquet writes:
- If nothing exists yet: write a single unversioned file.
- If only an unversioned file exists: copy that to `_v1`, then write the
  new data to the next version (typically `_v2`) and update the unversioned
  file to be a copy of the latest version.
- If one or more versioned files exist: write the new data to the next
  version and update the unversioned file to match the latest.

Note: On object stores like GCS there is no atomic rename; operations are
copy+delete under the hood. To minimize downtime we prefer copy semantics
for "migrating" the unversioned file to `_v1` (keep the unversioned until the
new version is written and re-pointed).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from dapla import FileClient

from fagfunksjoner.fagfunksjoner_logger import logger
from fagfunksjoner.paths.versions import (
    construct_file_pattern,
    get_fileversions,
    next_version_path,
)


def _is_remote(path: str) -> bool:
    return (
        path.startswith("gs://") or path.startswith("http") or path.startswith("ssb-")
    )


def _get_fs() -> Any:
    """Return the fsspec filesystem for remote storage (GCS)."""
    return FileClient.get_gcs_file_system()


def _exists(path: str) -> bool:
    if _is_remote(path):
        fs = _get_fs()
        res = fs.exists(path)
        return bool(res) if isinstance(res, bool) else False
    return Path(path).exists()


def _copy(src: str, dst: str) -> None:
    if _is_remote(src) or _is_remote(dst):
        fs = _get_fs()
        # gcsfs exposes `copy` for single files
        fs.copy(src, dst)
    else:
        from shutil import copy2

        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        copy2(src, dst)


def _delete(path: str) -> None:
    if _is_remote(path):
        fs = _get_fs()
        fs.delete(path)
    else:
        p = Path(path)
        if p.exists():
            p.unlink()


def _rename(src: str, dst: str) -> None:
    if _is_remote(src) or _is_remote(dst):
        fs = _get_fs()
        if hasattr(fs, "rename"):
            fs.rename(src, dst)
        elif hasattr(fs, "mv"):
            fs.mv(src, dst)
        else:
            fs.copy(src, dst)
            fs.delete(src)
    else:
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        Path(src).replace(dst)


def _write_parquet(
    df: pd.DataFrame, path: str, to_parquet_kwargs: dict[str, Any] | None = None
) -> None:
    kwargs: dict[str, Any] = {"index": False}
    if to_parquet_kwargs:
        kwargs.update(to_parquet_kwargs)
    # Ensure parent exists for local paths
    if not _is_remote(path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, **kwargs)


def _to_unversioned(path: str) -> str:
    # Mirrors the logic in versions.py to identify the unversioned name
    file_ext = f".{path.rsplit('.', 1)[-1]}" if "." in path else ""
    if "_v" in path:
        return path.rsplit("_v", 1)[0] + file_ext
    return path


def write_unversioned_and_versioned_parquet(
    df: pd.DataFrame,
    target_path: str | Path,
    *,
    dry_run: bool = False,
    prefer_copy_migration: bool = True,
    keep_unversioned: bool = True,
    to_parquet_kwargs: dict[str, Any] | None = None,
) -> list[str] | list[Path]:
    """Write a DataFrame to versioned and unversioned Parquet paths.

    Behavior when keep_unversioned=True (default):
    - No existing files: write only the unversioned path.
    - Only unversioned exists: copy it to `_v1`, then write new data to the
      next version and update unversioned to match latest.
    - Versioned exists (with/without unversioned): write new data to next
      version and update unversioned to match latest.

    Behavior when keep_unversioned=False (versioned-only):
    - No existing files: write `_v1` only.
    - Only unversioned exists: warn and rename unversioned -> `_v1`, then write
      new data to next version (typically `_v2`). Do not keep unversioned.
    - Versioned exists and unversioned exists: warn and delete unversioned, then
      write new data to next version. Do not keep unversioned.

    Args:
        df: DataFrame to write.
        target_path: Path-like (either versioned or unversioned form). The
            directory and base name are used to locate versions and unversioned.
        dry_run: If True, only logs intended actions and returns the paths
            that would be written. No IO is performed.
        prefer_copy_migration: If True, when migrating from only-unversioned
            to versioned layout, copy unversioned -> `_v1` to avoid downtime.
            If False, attempt a rename (not atomic across all backends).
        keep_unversioned: When True (default), writes/maintains an unversioned
            file as a mirror of the latest version. When False, only versioned
            files are written/kept and any existing unversioned is removed.
        to_parquet_kwargs: Extra keyword arguments passed to `df.to_parquet`.

    Returns:
        The list of paths that were written (or would be written in dry_run).
    """
    was_path = isinstance(target_path, Path)
    path_str = str(target_path)
    unversioned = _to_unversioned(path_str)

    existing = [str(x) for x in get_fileversions(path_str)]
    versioned_existing = [p for p in existing if "_v" in p]
    unversioned_exists = _exists(unversioned)

    written: list[str] = []

    if not versioned_existing and not unversioned_exists:
        if keep_unversioned:
            # Write only the unversioned file
            logger.info(f"No existing files. Writing unversioned file: {unversioned}")
            if not dry_run:
                _write_parquet(df, unversioned, to_parquet_kwargs)
            written.append(unversioned)
        else:
            # Write v1 only
            v1_path = str(next_version_path(path_str))
            logger.info(f"No existing files. Writing first versioned file: {v1_path}")
            if not dry_run:
                _write_parquet(df, v1_path, to_parquet_kwargs)
            written.append(v1_path)

    elif not versioned_existing and unversioned_exists:
        # Only unversioned exists
        v1_path = str(construct_file_pattern(unversioned, version_denoter="1"))
        next_v_path = str(next_version_path(path_str))  # typically v2

        if keep_unversioned:
            logger.info(
                f"Migrating unversioned to v1 using {'copy' if prefer_copy_migration else 'rename'}: {unversioned} -> {v1_path}"
            )
            if not dry_run:
                if prefer_copy_migration:
                    _copy(unversioned, v1_path)
                else:
                    _rename(unversioned, v1_path)
            # Write the new data to next version
            logger.info(f"Writing next versioned file: {next_v_path}")
            if not dry_run:
                _write_parquet(df, next_v_path, to_parquet_kwargs)
            written.append(next_v_path)
            # Update unversioned to latest by copying the new versioned file
            logger.info(
                f"Updating unversioned to latest by copying: {next_v_path} -> {unversioned}"
            )
            if not dry_run:
                _copy(next_v_path, unversioned)
            written.append(unversioned)
        else:
            logger.warning(
                "Unversioned file exists; moving to versioned layout only. Renaming to _v1 and not keeping unversioned."
            )
            if not dry_run:
                _rename(unversioned, v1_path)
            # Write new data to next version (v2)
            logger.info(f"Writing next versioned file: {next_v_path}")
            if not dry_run:
                _write_parquet(df, next_v_path, to_parquet_kwargs)
            written.append(next_v_path)

    else:
        # There are versioned files already
        next_v_path = str(next_version_path(path_str))
        logger.info(f"Writing next versioned file: {next_v_path}")
        if not dry_run:
            _write_parquet(df, next_v_path, to_parquet_kwargs)
        written.append(next_v_path)

        if keep_unversioned:
            logger.info(
                f"Updating unversioned to latest by copying: {next_v_path} -> {unversioned}"
            )
            if not dry_run:
                _copy(next_v_path, unversioned)
            written.append(unversioned)
        elif unversioned_exists:
            logger.warning(
                "Removing stale unversioned file; versioned-only policy active."
            )
            if not dry_run:
                _delete(unversioned)

    if was_path:
        return [Path(p) for p in written]
    return written


__all__ = [
    "write_unversioned_and_versioned_parquet",
]


def write_versioned_parquet(
    df: pd.DataFrame,
    target_path: str | Path,
    *,
    dry_run: bool = False,
    prefer_copy_migration: bool = True,
    to_parquet_kwargs: dict[str, Any] | None = None,
) -> list[str] | list[Path]:
    """Write DataFrame to versioned files only (no unversioned kept).

    Thin wrapper around `write_unversioned_and_versioned_parquet` with
    `keep_unversioned=False`.
    """
    return write_unversioned_and_versioned_parquet(
        df,
        target_path,
        dry_run=dry_run,
        prefer_copy_migration=prefer_copy_migration,
        keep_unversioned=False,
        to_parquet_kwargs=to_parquet_kwargs,
    )
