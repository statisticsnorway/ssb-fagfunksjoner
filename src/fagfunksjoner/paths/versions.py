"""This module works with filepaths, and is especially aimed at GCS and Dapla.

But it works locally as well if standard for versionizing datafiles are implemented.
The main purpose is fileversions according to Statistics Norway standards.
"""

import glob

from dapla import FileClient

from fagfunksjoner.fagfunksjoner_logger import logger


def get_latest_fileversions(glob_list_path: list[str] | str) -> list[str]:
    """Recieves a list of filenames with multiple versions, and returns the latest versions of the files.

    Recommend using glob operation to create the input list.
    See doc for glob operations:
    - GCS: https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem.glob
    - Locally: https://docs.python.org/3/library/glob.html

    Args:
        glob_list_path: List of strings or single string that represents a filepath.
            Recommend that the list is created with glob operation.

    Returns:
        list[str]: List of strings with unique filepaths and its latest versions

    Raises:
        TypeError: If parameter does not fit with type-narrowing to list of strings.

    Example::

            import dapla as dp
            fs = dp.FileClient.get_gcs_file_system()
            all_files = fs.glob("gs://dir/statdata_v*.parquet")
            latest_files = get_latest_fileversions(all_files)
    """
    if isinstance(glob_list_path, str):
        infiles = [glob_list_path]
    elif isinstance(glob_list_path, list):
        infiles = glob_list_path
    else:
        raise TypeError("Expecting glob_list_path to be a str or a list of str.")

    # Extract unique base names by splitting before the version part
    uniques = set(file.rsplit("_v", 1)[0] for file in infiles)
    result = []

    for unique in uniques:
        # Collect all entries that match the current unique base name
        entries = [x for x in infiles if x.startswith(unique + "_v")]
        unique_sorter = []

        for entry in entries:
            try:
                # Extract version number from the file name
                version_number = int(entry.split("_v")[-1].split(".")[0])
                unique_sorter.append((version_number, entry))
            except ValueError as v:
                logger.warning(
                    f"Cannot extract file version from file stem {entry}: {v}"
                )

        # Sort the collected entries by version number and get the latest one
        if unique_sorter:
            latest_entry = max(unique_sorter, key=lambda x: x[0])[1]
            result.append(latest_entry)

    return result


def latest_version_number(filepath: str) -> int:
    """Function for finding latest version in use for a file.

    Args:
        filepath: GCS filepath or local filepath, should be the full path, but needs to follow the naming standard.
            eg. ssb-prod-ofi-skatteregn-data-produkt/skatteregn/inndata/skd_data/2023/skd_p2023-01_v1.parquet
            or /ssb/stammeXX/kortkode/inndata/skd_data/2023/skd_p2023-01_v1.parquet

    Returns:
        int: The latest version number for the file.
    """
    file_no_version, old_version, file_ext = split_path(filepath)
    glob_pattern = f"{file_no_version}v*{file_ext}"

    if (
        filepath.startswith("gs://")
        or filepath.startswith("http")
        or filepath.startswith("ssb-")
    ):
        fs = FileClient.get_gcs_file_system()
        files = fs.glob(glob_pattern)
    else:
        files = glob.glob(glob_pattern)
    if files:
        logger.info(f"Found {len(files)} files: {files}")
        latest_file = get_latest_fileversions(files)[-1]
    else:
        logger.warning(
            f"""Cant find any files with this name, glob-pattern: {glob_pattern} Found files: {files}"""
        )
        version_number = int(input("Which version number do you want to use?"))
        latest_file = f"{file_no_version}v{version_number}{file_ext}"

    _file_no_version, latest_version, _file_ext = split_path(latest_file)
    latest_version_int = int("".join([c for c in latest_version if c.isdigit()]))

    if latest_version_int - int(old_version[1:]) > 0:
        logger.warning(
            f"""You specified a path with version {old_version}, but we found a version {latest_version_int}.
                       Are you sure you are working from the latest version?"""
        )

    return latest_version_int


def latest_version_path(filepath: str) -> str:
    """Finds the latest version of the specified file.

    Args:
        filepath: The address for the file.

    Returns:
        str: The file path in use of the highest version.
    """
    latest_number_int = latest_version_number(filepath)
    file_no_version, _old_version, file_ext = split_path(filepath)
    latest_path = f"{file_no_version}v{latest_number_int}{file_ext}"
    return latest_path


def next_version_number(filepath: str) -> int:
    """Function for finding next version for a new file.

    Args:
        filepath: GCS filepath or local filepath, should be the full path, but needs to follow the naming standard.
            eg. ssb-prod-ofi-skatteregn-data-produkt/skatteregn/inndata/skd_data/2023/skd_p2023-01_v1.parquet
            or /ssb/stammeXX/kortkode/inndata/skd_data/2023/skd_p2023-01_v1.parquet

    Returns:
        int: The next version number for the file.
    """
    latest_version = latest_version_number(filepath)
    if latest_version == 0:
        next_version_int = 0
    else:
        next_version_int = 1 + latest_version
    return next_version_int


def next_version_path(filepath: str) -> str:
    """Generates a new file path with an incremented version number.

    Constructs a filepath for a new version of a file,
    based on the latest existing version found in a specified folder.
    Meaning it skips to "one after the highest version it finds".
    It increments the version number by one, to ensure the new file path is unique.

    Args:
        filepath: The address for the file.

    Returns:
        str: The new file path with an incremented version number and specified suffix.

    Example::

        get_new_filename_and_path('gs://my-bucket/datasets/data_v1.parquet')
        'gs://my-bucket/datasets/data_v2.parquet'
    """
    next_version_number_int = next_version_number(filepath)
    file_no_version, _old_version, file_ext = split_path(filepath)
    new_path = f"{file_no_version}v{next_version_number_int}{file_ext}"
    return new_path


def split_path(filepath: str) -> tuple[str, str, str]:
    """Split the filepath into three pieces, version, file-extension and the rest.

    Args:
        filepath: The path you want split into pieces.

    Raises:
        ValueError: If the version-part doesnt follow the naming standard.

    Returns:
        tuple[str, str, str]: The parts of the path, for easy unpacking.
    """
    file_no_ext, file_ext = filepath.rsplit(".", 1)
    file_no_version, version = file_no_ext.rsplit("_", 1)

    if version[0] != "v" or not version[1:].isdigit():
        err = f"Version not following standard: '{version}', should start with v and the rest should be digits. "
        raise ValueError(err)

    file_no_version = f"{file_no_version}_"
    file_ext = f".{file_ext}"

    return file_no_version, version, file_ext
