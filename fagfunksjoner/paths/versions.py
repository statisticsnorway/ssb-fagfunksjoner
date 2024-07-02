"""This module works with filepaths, and is especially aimed at GCS and Dapla,
but it works locally as well if standard for versionizing datafiles are implemented.
The main purpose is fileversions according to Statistics Norway standards.
"""
import dapla as dp
from dapla import FileClient

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

def get_next_version_number(filepath: str) -> int:
    """
    Function for finding next version for a new file.

    Parameters
    ----------
    filepath: str
        GCS filepath. Must not include version suffix.
        eg. ssb-prod-ofi-skatteregn-data-produkt/skatteregn/inndata/skd_data/2023/skd_p2023-01.parquet

    Returns
    -------
    next_version_number: int
        The next version number for the file.
    """
    if filepath.startswith("gs://"):
        filepath = filepath[5:]
    fs = FileClient.get_gcs_file_system()
    folder_path = filepath.rsplit("/", 1)[0] + "/"
    file_name = filepath.rsplit("/", 1)[1].split(".")[0]
    base_name = file_name.split("_v")[0]
    
    try:
        files = fs.ls(folder_path)
    except Exception as e:
        # Log the exception if needed
        print(f"Error accessing file system: {e}")
        return 1
    
    version_numbers = []
    for path in files:
        if path.startswith(folder_path + base_name):
            parts = path.rsplit("_v", 1)
            if len(parts) == 2 and parts[1].split(".")[0].isdigit():
                version_numbers.append(int(parts[1].split(".")[0]))
    
    next_version_number = max(version_numbers, default=0) + 1
    
    return next_version_number
