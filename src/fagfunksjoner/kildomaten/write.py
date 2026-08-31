import re


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
