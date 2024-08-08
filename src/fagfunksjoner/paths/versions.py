"""This module works with filepaths, and is especially aimed at GCS and Dapla.

But it works locally as well if standard for versionizing datafiles are implemented.
The main purpose is fileversions according to Statistics Norway standards.
"""

import glob

from dapla import FileClient

from fagfunksjoner.fagfunksjoner_logger import logger
from fagfunksjoner.prodsone.check_env import check_env


def get_latest_fileversions(glob_list_path: list[str]) -> list[str]:
    """Recieves a list of filenames with multiple versions, and returns the latest versions of the files.

    Recommend using glob operation to create the input list.
    See doc for glob operations:
    - GCS: https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem.glob
    - Locally: https://docs.python.org/3/library/glob.html

    Args:
        glob_list_path (list[str]): List of strings that represents a filepath.
            Recommend that the list is created with glob operation.

    Returns:
        list[str]: List of strings with unique filepaths and its latest versions


    Example::

            import dapla as dp
            fs = dp.FileClient.get_gcs_file_system()
            all_files = fs.glob("gs://dir/statdata_v*.parquet")
            latest_files = get_latest_fileversions(all_files)
    """
    return [
        sorted([file for file in glob_list_path if file.startswith(unique)])[-1]
        for unique in sorted(
            list({file[0] for file in [file.split("_v") for file in glob_list_path]})
        )
    ]


def latest_version_number(filepath: str) -> int:
    """Function for finding latest version in use for a file.

    Args:
        filepath (str): GCS filepath or local filepath, should be the full path, but needs to follow the naming standard.
            eg. ssb-prod-ofi-skatteregn-data-produkt/skatteregn/inndata/skd_data/2023/skd_p2023-01_v1.parquet
            or /ssb/stammeXX/kortkode/inndata/skd_data/2023/skd_p2023-01_v1.parquet

    Returns:
        int: The latest version number for the file.
    """
    if filepath.startswith("gs://"):
        filepath = filepath[5:]

    file_no_version, old_version, file_ext = split_path(filepath)
    glob_pattern = f"{file_no_version}v*{file_ext}"

    if check_env(raise_err=False) == "DAPLA":
        fs = FileClient.get_gcs_file_system()
        files = fs.glob(glob_pattern)
    else:
        files = glob.glob(glob_pattern)
    if files:
        logger.info(f"Found this list of files: {files}")
        latest_file = sorted(files)[-1]
    else:
        logger.info(
            f"""Cant find any files with this name, setting existing version to v0 (should not exist, go straight to v1.
                        Glob-pattern: {glob_pattern} Found files: {files}"""
        )
        latest_file = f"{file_no_version}v0{file_ext}"

    _file_no_version, latest_version, _file_ext = split_path(latest_file)
    latest_version_int = int("".join([c for c in latest_version if c.isdigit()]))

    if latest_version_int - int(old_version[1:]) > 0:
        logger.warning(
            f"""You specified a path with version {old_version}, but we found a version {latest_version_int}.
                       Are you sure you are working from the latest version?"""
        )

    return latest_version_int


def next_version_number(filepath: str) -> int:
    """Function for finding next version for a new file.

    Args:
        filepath (str): GCS filepath or local filepath, should be the full path, but needs to follow the naming standard.
            eg. ssb-prod-ofi-skatteregn-data-produkt/skatteregn/inndata/skd_data/2023/skd_p2023-01_v1.parquet
            or /ssb/stammeXX/kortkode/inndata/skd_data/2023/skd_p2023-01_v1.parquet

    Returns:
        int: The next version number for the file.
    """
    next_version_int = 1 + latest_version_number(filepath)
    return next_version_int


def next_version_path(filepath: str) -> str:
    """Generates a new file path with an incremented version number.

    Constructs a filepath for a new version of a file,
    based on the latest existing version found in a specified folder.
    Meaning it skips to "one after the highest version it finds".
    It increments the version number by one, to ensure the new file path is unique.

    Args:
        filepath (str): The address for the file.

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
        filepath (str): The path you want split into pieces.

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
