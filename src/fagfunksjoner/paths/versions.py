"""This module works with filepaths and the versioning convention at SSB.

The main purpose is fileversions according to Statistics Norway standards.
The aim is to help versioning up and getting the latest version of paths in use on storage.

The module is not targeted at files that do not follow the naming convention of versions,
for example the __DOC.json-files, will not work, because they do not end with "_v1" before the file extension.
"""

import glob
from pathlib import Path
from typing import overload

from dapla import FileClient

from fagfunksjoner.fagfunksjoner_logger import logger, silence_logger


def _to_unversioned_path(filepath: str | Path) -> str:
    """Return the unversioned variant of a filepath.

    Examples:
        '/dir/file_v2.parquet' -> '/dir/file.parquet'
        '/dir/file.parquet' -> '/dir/file.parquet'
    """
    file_str = str(filepath)
    file_ext = f".{file_str.rsplit('.', 1)[-1]}" if "." in file_str else ""
    if "_v" in file_str:
        return file_str.rsplit("_v", 1)[0] + file_ext
    return file_str


def _path_exists(path: str) -> bool:
    """Check if a path exists either locally or in remote storage.

    Uses `FileClient.get_gcs_file_system().exists` for remote paths and
    `Path(path).exists()` for local paths.
    """
    try:
        if (
            path.startswith("gs://")
            or path.startswith("http")
            or path.startswith("ssb-")
        ):
            fs = FileClient.get_gcs_file_system()
            # Most fsspec filesystems expose an `exists` method
            res = fs.exists(path)
            # Only trust explicit booleans; otherwise treat as not existing
            return res if isinstance(res, bool) else False
        return Path(path).exists()
    except Exception:
        # Be conservative if we cannot check; assume it doesn't exist
        return False


def get_version_number(filepath: str | Path) -> int:
    """Extracts the version number from a given file path.

    This function parses the file path to retrieve the version number, which should be indicated using '_v' followed by digits before the file extension.
    For example, a valid file path would be 'file_v1.parquet'.
    If the naming convention is not followed, a ValueError is raised.

    Args:
        filepath: The file path string containing the version information.

    Returns:
        int: The extracted version number as an integer.

    Raises:
        ValueError: If the filepath does not contain '_v' followed by digits.
    """
    file_str = str(filepath)
    # Ensure the input is a string and extract the version part.
    if not isinstance(file_str, str):
        raise ValueError(
            f"Expected a string at this point for filepath, got {type(file_str)}: {file_str}"
        )

    # Extract the version number by splitting the string at '_v' and '.'
    version_str = ""
    if "_v" in file_str:
        after_v = file_str.rsplit("_v", 1)[1].split(".")[0]
        # There might be things after _v in the case of metadata files, like "__DOC"
        for c in after_v:
            if not c.isdigit():
                break
            version_str += c

    # Check if '_v' is in the filepath and if the extracted version is a valid digit.
    if "_v" not in file_str or not version_str.isdigit():
        err = (
            f"Filepath does not follow standard naming convention: '{file_str}'. "
            "Use '_v' followed by digits to denote file version."
        )
        # Raise a ValueError if the naming convention is not followed.
        raise ValueError(err)

    # Return the version number as an integer.
    return int(version_str)


def get_file_name(filepath: str | Path) -> str:
    """Extracts the base file name from a given file path, excluding the version number.

    This function extracts the file name before the '_v' version indicator
    and removes any preceding directory path. For example, if the input is
    'path/to/file_v1.parquet', it will return 'file'.

    Args:
        filepath: The file path string containing the file name and version information.

    Returns:
        str: The base file name without the version number and directory path.
    """
    file_str = str(filepath)
    # Split the string at '_v' and take the first part (before the version number).
    # Then, split again at the last '/' to isolate the base file name.
    base_file_name = file_str.rsplit("_v", 1)[0].rsplit("/", 1)[-1]

    # Return the extracted base file name.
    return base_file_name


@overload
def get_latest_fileversions(glob_list_path: Path) -> list[Path]: ...
@overload
def get_latest_fileversions(glob_list_path: str) -> list[str]: ...
@overload
def get_latest_fileversions(glob_list_path: list[str]) -> list[str]: ...
@overload
def get_latest_fileversions(glob_list_path: list[Path]) -> list[Path]: ...


def get_latest_fileversions(
    glob_list_path: list[str] | list[Path] | str | Path,
) -> list[str] | list[Path]:
    """Receives a list of filenames with multiple versions and returns the latest versions of the files.

    Recommend using glob operation to create the input list.
    See doc for glob operations:
    - GCS: https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem.glob
    - Locally: https://docs.python.org/3/library/glob.html

    Args:
        glob_list_path: List of strings/Paths or single string/Path that represents a filepath.
            Recommend that the list is created with glob operation.

    Returns:
        list[str | Path]: List of strings, or Paths (if path was submitted) with unique filepaths and their latest versions.

    Raises:
        TypeError: If parameter does not fit with type-narrowing to list of strings.

    Example::

            import dapla as dp
            fs = dp.FileClient.get_gcs_file_system()
            all_files = fs.glob("gs://dir/statdata_v*.parquet")
            latest_files = get_latest_fileversions(all_files)
    """
    if isinstance(glob_list_path, str | Path):
        was_path = isinstance(glob_list_path, Path)
        infiles: list[str] = [str(glob_list_path)]
    elif isinstance(glob_list_path, list):
        was_path = all([isinstance(x, Path) for x in glob_list_path])
        infiles = [str(x) for x in glob_list_path]
    else:
        raise TypeError("Expecting glob_list_path to be a str or a list of str.")

    uniques = set()
    # Compensate for what might be after the version, like "__DOC" in metadata files
    for file in infiles:
        if "_v" in file:
            base_name = file.rsplit("_v", 1)[0]
            after_v = file.rsplit("_v", 1)[1]
            i = [char.isdigit() for char in after_v].index(False)
            uniques.add(base_name + "*" + after_v[i:])
        else:
            logger.info(
                f"File {file} does not follow the naming convention with '_v' for versioning."
            )

    result = _get_entries_by_uniques(uniques, infiles)

    if was_path:
        return [Path(file) for file in result]
    return result


def _get_entries_by_uniques(uniques: set[str], infiles: list[str]) -> list[str]:
    result: list[str] = []
    for unique in uniques:
        # Collect all entries that match the current unique base name
        entries = [
            x
            for x in infiles
            if "_v" in x
            and x.rsplit("_v", 1)[0] == unique.rsplit("*", 1)[0]
            and x.endswith(unique.rsplit("*", 1)[-1])
        ]  # Characters after match is only digits
        unique_sorter = []

        for entry in entries:
            try:
                # Extract version number using the get_version_number function
                version_number = get_version_number(entry)
                unique_sorter.append((version_number, entry))
            except ValueError as v:
                logger.warning(
                    f"Cannot extract file version from file stem {entry}: {v}"
                )
        # Sort the collected entries by version number and get the latest one
        if unique_sorter:
            latest_entry = max(unique_sorter, key=lambda x: x[0])[1]
            logger.info(f"Latest version(s): {latest_entry.rsplit('/', 1)[-1]}")
            result.append(latest_entry)
    return result


def construct_file_pattern(filepath: str | Path, version_denoter: str = "*") -> str:
    """Constructs a file pattern for versioned file paths.

    This function generates a file pattern by extracting the base file name and its extension,
    allowing the version part to be replaced by a specified version denoter (default is '*').
    If the filepath does not contain an extension, '.parquet' is assumed.

    Args:
        filepath: The input file path with a version number.
        version_denoter: A placeholder for the version number in the pattern (default is '*').

    Returns:
        str: The constructed file pattern with the version denoter in place of the actual version.
    """
    file_str = str(filepath)
    # Extract the file extension or assume '.parquet' if none is present.
    file_ext = f".{file_str.rsplit('.', 1)[-1]}" if "." in file_str else ".parquet"

    # Remove the version part if present, or strip the extension otherwise.
    filepath_no_version = (
        file_str.rsplit("_v", 1)[0]
        if "_v" in file_str
        else file_str.replace(file_ext, "")
    )
    # Compensate for what might be after the version, like "__DOC" in metadata files
    extras = ""
    if "_v" in file_str:
        after_v = file_str.rsplit("_v", 1)[1].rsplit(".", 1)[0]
        # Find the first non-digit character in the version part to separate it from any extras.
        look_for_non_digit = [c.isdigit() for c in after_v]
        if False in look_for_non_digit:
            i = look_for_non_digit.index(False)
            extras = after_v[i:]

    # Construct the file pattern by inserting the version denoter.
    glob_pattern = f"{filepath_no_version}_v{version_denoter}{extras}{file_ext}"
    logger.debug(f"glob_pattern = {glob_pattern}")
    return glob_pattern


@overload
def get_fileversions(filepath: Path) -> list[Path]: ...
@overload
def get_fileversions(filepath: str) -> list[str]: ...


def get_fileversions(filepath: str | Path) -> list[str] | list[Path]:
    """Retrieves a list of file versions matching a specified pattern.

    This function generates a glob pattern based on the provided file path and retrieves
    all matching versions. It supports both local files and files stored in Google Cloud
    Storage (GCS). If the filepath points to a cloud location (e.g., starting with 'gs://',
    'http', or 'ssb-'), it uses a GCS file system client to find matches; otherwise, it
    searches for files locally using the glob module.

    Args:
        filepath: The input file path with a version indicator.

    Returns:
        A list of file paths matching the version pattern.
    """
    was_path = isinstance(filepath, Path)
    file_str = str(filepath)
    # Construct a file pattern with a wildcard version denoter using the input filepath.
    glob_pattern = construct_file_pattern(file_str)
    # Determine the appropriate file system client based on the filepath's prefix.
    if (
        file_str.startswith("gs://")
        or file_str.startswith("http")
        or file_str.startswith("ssb-")
    ):
        # Use a GCS file system client for cloud storage files.
        fs = FileClient.get_gcs_file_system()
        files_list = fs.glob(glob_pattern)
    else:
        # Use the standard glob module for local files.
        files_list = glob.glob(glob_pattern)

    # Extract the base file name from the glob pattern for logging purposes.
    base_file_name = get_file_name(glob_pattern)

    # Check if any files were found.
    if files_list:
        # Log the number of found versions and return the list of files.
        logger.info(f"Found {len(files_list)} versions of file: {base_file_name}")
    else:
        # Log a warning if no files were found with the given pattern and return None.
        logger.warning(
            f"Can't find any files with this name, glob-pattern: {glob_pattern}."
        )
    if was_path:
        # If the original filepath was a Path object, convert the list of file paths to Path objects.
        return [Path(file) for file in files_list]
    return list(files_list)


@overload
def latest_version_path(filepath: Path) -> Path: ...
@overload
def latest_version_path(filepath: str) -> str: ...


def latest_version_path(filepath: str | Path) -> str | Path:
    """Finds the path to the latest version of a specified file.

    This function retrieves all versioned files matching the provided file path pattern
    and identifies the latest version. It supports both Google Cloud Storage (GCS) paths
    and local file paths, provided they follow the required naming convention with version
    numbers (e.g., '_v1'). If no versions are found, it defaults to returning a pattern
    representing version 1.

    Args:
        filepath: The full path of the file, either a GCS path or a local path.
            It should follow the naming standard, including the version indicator.

    Returns:
        str | Path: The path to the latest version of the file. If no versions are found, returns
             a pattern for version 1 of the file.

    Raises:
        ValueError: If `get_latest_fileversions` returns a list of more than one file.
        ValueError: If the filepath does not follow the naming convention with '_v'
                    followed by digits to denote version, when a versioned file is required.

    Examples:
        - 'ssb-prod-ofi-skatteregn-data-produkt/skatteregn/inndata/skd_data/2023/skd_p2023-01_v1.parquet'
        - '/ssb/stammeXX/kortkode/inndata/skd_data/2023/skd_p2023-01_v1.parquet'
    """
    was_path = isinstance(filepath, Path)
    file_str = str(filepath)
    # Retrieve all file versions matching the given filepath pattern.
    files_list = get_fileversions(file_str)
    logger.info(f"Files_list: {files_list}")

    # If entries are found, prefer versioned entries. If only unversioned are present, return them.
    if files_list:
        # Separate versioned and unversioned candidates (supporting patched tests and edge-cases)
        versioned = [f for f in files_list if "_v" in str(f)]
        unversioned_candidates = [f for f in files_list if "_v" not in str(f)]

        if versioned:
            # Get the latest version from versioned entries only
            latest_files_list = get_latest_fileversions(versioned)
            logger.info(f"Latest_files_list: {latest_files_list}")

            if len(latest_files_list) > 1:
                list_print = [file.rsplit("/", 1) for file in latest_files_list]
                raise ValueError(
                    f"The latest version returned more than one file: {list_print}"
                )

            latest_file = latest_files_list[0]

            # Extract the version number from the latest file.
            latest_version_number = get_version_number(latest_file)

            # Log the detected latest version number.
            logger.info(f"Latest version of file is number {latest_version_number}.")

            # Check if the specified filepath contains a version number.
            if "_v" in file_str:
                # Extract the version number from the specified filepath.
                specified_version = get_version_number(file_str)

                # Compare the specified version with the detected latest version.
                if latest_version_number > specified_version:
                    # Warn the user if the specified version is not the latest.
                    logger.warning(
                        f"You specified a path with version {specified_version}, but we found a version {latest_version_number}. "
                        "Are you sure you are working from the latest version?"
                    )

            # Return the path to the latest version of the file.
            if was_path:
                return Path(latest_file)
            return latest_file

        # No versioned entries, but something exists; return the unversioned entry (prefer first)
        if unversioned_candidates:
            if was_path:
                return Path(str(unversioned_candidates[0]))
            return str(unversioned_candidates[0])

    else:
        # Construct a pattern for version 1 if no versions are found.
        # Before defaulting to v1 pattern, check if the unversioned file exists and return it
        unversioned_path = _to_unversioned_path(file_str)
        if _path_exists(unversioned_path):
            logger.info(
                "No versioned files found; returning unversioned file as latest."
            )
            if was_path:
                return Path(unversioned_path)
            return unversioned_path

        filepath_default = construct_file_pattern(
            filepath=file_str, version_denoter="1"
        )

        # Inform the user that version 1 is being returned as a default.
        logger.info("No versions of the file were found. Version 1 was returned.")

        # Return the default pattern for version 1.
        if was_path:
            return Path(filepath_default)
        return filepath_default

    # Fallback: if control reaches here (shouldn't normally), return v1 pattern
    filepath_default = construct_file_pattern(filepath=file_str, version_denoter="1")
    if was_path:
        return Path(filepath_default)
    return filepath_default


def latest_version_number(filepath: str | Path) -> int:
    """Find the latest version number for a file.

    If versioned files exist, returns their highest version number.
    If only an unversioned file exists, treat it as version 1.
    If nothing exists, default to 1.
    """
    latest = latest_version_path(str(filepath))
    latest_str = str(latest)
    if "_v" in latest_str:
        return get_version_number(latest_str)
    # Unversioned or default pattern; treat as version 1
    return 1


def next_version_number(filepath: str | Path) -> int:
    """Find the next version number for a new file.

    - If versioned files exist, returns highest + 1.
    - If only an unversioned file exists, warn and return 2.
    - If nothing exists, return 1.
    """
    file_str = str(filepath)
    versions = silence_logger(get_fileversions, file_str)

    if versions:
        # If versions include at least one versioned file, base on those; otherwise fall through
        versioned = [f for f in versions if "_v" in str(f)]
        if versioned:
            current_version_int = latest_version_number(file_str)
            return current_version_int + 1

    # No versioned files found; check for unversioned file
    unversioned = _to_unversioned_path(file_str)
    if _path_exists(unversioned):
        logger.warning(
            "Unversioned file exists alongside request for next version. "
            "Consider renaming it to '_v1' and maintaining 'file' as a copy of latest."
        )
        return 2

    logger.info("Did not find any existing versions of the file. Starting at v1.")
    return 1


@overload
def next_version_path(filepath: Path, include_unversioned: bool = False) -> Path: ...
@overload
def next_version_path(filepath: str, include_unversioned: bool = False) -> str: ...
@overload
def next_version_path(
    filepath: Path, include_unversioned: bool = True
) -> tuple[Path, Path]: ...
@overload
def next_version_path(
    filepath: str, include_unversioned: bool = True
) -> tuple[str, str]: ...


def next_version_path(
    filepath: str | Path, include_unversioned: bool = False
) -> str | Path | tuple[str, str] | tuple[Path, Path]:
    """Generates a new file path with an incremented version number.

    Constructs a filepath for a new version of a file,
    based on the latest existing version found in a specified folder.
    Meaning it skips to "one after the highest version it finds".
    It increments the version number by one, to ensure the new file path is unique.

    Args:
        filepath: The path for the file.
        include_unversioned: If we should include the unversioned path in a tuple when we return

    Returns:
        str | Path: The new file path with an incremented version number and specified suffix.

    Example::

        next_version_path('gs://my-bucket/datasets/data_v1.parquet')
        'gs://my-bucket/datasets/data_v2.parquet'
    """
    was_path = isinstance(filepath, Path)
    file_str = str(filepath)

    # Determine the next version number
    next_version_number_int = next_version_number(file_str)

    # Build components similar to construct_file_pattern to preserve extras
    file_ext = f".{file_str.rsplit('.', 1)[-1]}" if "." in file_str else ".parquet"
    filepath_no_version = (
        file_str.rsplit("_v", 1)[0]
        if "_v" in file_str
        else file_str.replace(file_ext, "")
    )
    extras = ""
    if "_v" in file_str:
        after_v = file_str.rsplit("_v", 1)[1].rsplit(".", 1)[0]
        look_for_non_digit = [c.isdigit() for c in after_v]
        if False in look_for_non_digit:
            i = look_for_non_digit.index(False)
            extras = after_v[i:]

    # Construct new versioned path
    new_path = f"{filepath_no_version}_v{next_version_number_int}{extras}{file_ext}"

    next_base_file_name = get_file_name(new_path)
    logger.info(
        f"The next version of file {next_base_file_name} is {next_version_number_int}."
    )

    if was_path:
        if include_unversioned:
            return (
                Path(new_path),
                Path(filepath_no_version + file_ext),
            )
        return Path(new_path)

    if include_unversioned:
        return (
            new_path,
            filepath_no_version + file_ext,
        )
    return new_path


def split_path(filepath: str | Path) -> tuple[str, str, str]:
    """Split the filepath into three pieces, version, file-extension and the rest.

    Args:
        filepath: The path you want split into pieces.

    Raises:
        ValueError: If the version-part doesnt follow the naming standard.

    Returns:
        tuple[str, str, str]: The parts of the path, for easy unpacking.
    """
    file_str = str(filepath)
    file_no_ext, file_ext = file_str.rsplit(".", 1)
    file_no_version, version = file_no_ext.rsplit("_", 1)

    if version[0] != "v" or not version[1:].isdigit():
        err = f"Version not following standard: '{version}', should start with v and the rest should be digits. "
        raise ValueError(err)

    file_no_version = f"{file_no_version}_"
    file_ext = f".{file_ext}"

    return file_no_version, version, file_ext
