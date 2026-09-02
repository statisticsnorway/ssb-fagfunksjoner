import re
from pathlib import Path

from fagfunksjoner.paths.versions import next_version_path

from .config import KildomatConfig


def build_output_name(source_filename: str, insert: str = "_inndata_") -> str:
    """Build the output filename for a source file.

    Args:
        source_filename: Source filename to transform.
        insert: Filename marker to insert or preserve.

    Returns:
        str: Output filename with the requested marker.
    """
    if insert in source_filename:
        return source_filename
    if "_kilde_" in source_filename:
        return source_filename.replace("_kilde_", insert)
    return re.sub(
        r"_p(?=\d{4})", f"{insert}p", source_filename, count=1, flags=re.IGNORECASE
    )


def _resolve_output_path(
    source_path: Path | None,
    file_config: KildomatConfig,
    *,
    dry_run: bool = False,
) -> Path:
    if file_config.output_path:
        return file_config.output_path

    if source_path is None:
        if dry_run:
            return Path("dry_run_output.parquet")
        raise ValueError("file_config.output_path is required for DataFrame input")

    output_name = build_output_name(
        source_path.name,
        insert=file_config.output_name_insert,
    )
    output_dir = file_config.output_dir or source_path.parent
    if not file_config.output_overwrite:
        return Path(next_version_path(output_dir / output_name))
    return output_dir / output_name
