"""This module works with filepaths, and is especially aimed at GCS and Dapla,
but it works locally as well if standard for versionizing datafiles are implemented.
The main purpose is fileversions according to Statistics Norway standards.
"""


def get_latest_fileversions(glob_list_path: list[str]) -> list[str]:
    """Recieves a list of filenames with multiple versions,
    and returns the latest versions of the files.
    Recommend using glob operation to create the input list.
    See doc for glob operations:
    - GCS: https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem.glob
    - Locally: https://docs.python.org/3/library/glob.html

    Example:
    import dapla as dp
    fs = dp.FileClient.get_gcs_file_system()
    all_files = fs.glob("gs://dir/statdata_v*.parquet")
    latest_files = get_latest_fileversions(all_files)
    
    Parameters
    ----------
    glob_list_path: list[str]
        List of strings that represents a filepath.
        Recommend that the list is created with glob operation.

    Returns
    -------
    list[str]
        List of strings with unique filepaths and its latest versions
    """
    return [sorted([file for file in glob_list_path if file.startswith(unique)])[-1]
            for unique in
            sorted(list(set([file[0] for file in [file.split('_v') for file in glob_list_path]])))]
