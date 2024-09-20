"""This module works with filepaths, and is especially aimed at GCS and Dapla.

But it works locally as well if standard for versionizing datafiles are implemented.
The main purpose is fileversions according to Statistics Norway standards.
"""

import glob
from enum import Enum

import pandas as pd
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
        glob_list_path: List of strings that represents a filepath.
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
        filepath: GCS filepath or local filepath, should be the full path, but needs to follow the naming standard.
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
        filepath: GCS filepath or local filepath, should be the full path, but needs to follow the naming standard.
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


class OrderBy(Enum):
    """Represents the possibilities for file filters in
    function get_latest_gcs_files
    """
    VERSION = 'version'
    DATE = 'date'


def get_latest_gcs_files(gcs_fileblob_prefix: str,
                         by: str = 'version',
                         file_format: str = 'parquet',
                         date_filter: str = None,
                         detail: bool = False) -> list[str] | dict[str, str]:
    """Get latest files from GCS either by date og version if SSB standards for filenames is followed.

    Helps you get latest files in a gcs bucket, where there will be a lot of files.
    You either get latest by date of creation of the file, or by the file version.
    File version depends on if SSB standards for filenames are followed.

    Args:
        gcs_fileblob_prefix: Filename prefix in GCS bucket. This includes filename
                             description, and periods. This goes in to a glob search,
                             so it is possible to use glob signs like * and ?.
        by: Choose between 'version' or 'date', default 'version'.
        file_format: The files fileformat, default 'parquet'.
        date_filter: If you want to filter by date greater than, e.g. '2024-09-20'.
        detail: If you want a bit more details or not.

    Returns:
        list[str] | dict[str, str]: List of filenames sorted in ascending order.
                                    If detail is True, then a dictionary is returned,
                                    with filename er key, and datetime in string as value.


    Example::

            # Normal easy usecase
            KLARGJORT = "bucket/path/to/file/file-desc-name_pYYYY-MM-DD_pYYYY-MM-DD"
            latest_files = get_latest_gcs_files(KLARGJORT)

            # I want newst files since some date
            latest_files = get_latest_gcs_files(KLARGJORT, by='date', date_filter='2024-09-19')
    """
    
    dapla_fs = FileClient.get_gcs_file_system()
    orderby = OrderBy(by)
    
    files = dapla_fs.glob(f"{gcs_fileblob_prefix}*.{file_format}", detail=True)
    files_df = pd.DataFrame.from_records(list(files.values()), columns=['name', 'mtime'])
    files_df = files_df.sort_values('mtime', ascending=True, ignore_index=True)
    
    if date_filter is not None:
        files_df = files_df.loc[files_df.mtime >= date_filter]
    
    match orderby:
        
        case orderby.VERSION:
            
            files_df['file_name'] = files_df.name.str.split('/').str[-1]
            files_df['file_desc'] = files_df.file_name.str.split('_v').str[0]
            files_df['version'] = files_df.file_name.str.split('_v').str[1].str.split('.').str[0]
            files_df = files_df.dropna(ignore_index=True).sort_values('mtime', ascending=True, ignore_index=True)
            
            files_df2 = (
                files_df
                .groupby('file_desc', as_index=False)
                .agg({'version': 'max'})
                .merge(files_df, on=['file_desc', 'version'], how='left')
                .sort_values('mtime', ascending=True, ignore_index=True)
            )
            
            if detail:
                files_df2 = files_df2[['name', 'mtime']]
                allfiles = files_df2.set_index('name').to_dict('dict').get('mtime')
                
            else:
                allfiles = files_df2.name.tolist()
        
        case orderby.DATE:
            
            if detail:
                files_df = files_df[['name', 'mtime']]
                allfiles = files_df.set_index('name').to_dict('dict').get('mtime')
                
            else:
                allfiles = files_df.name.tolist()
            
    return allfiles
