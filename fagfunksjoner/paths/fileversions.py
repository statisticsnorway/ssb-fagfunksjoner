"""This module works with filepaths, and is especially aimed at GCS and Dapla,
but it works locally as well. The main purpose is fileversions according to
Statistics Norway standards.
"""


def get_latest_fileversions(glob_list_path: list[str]) -> list[str]:
    """Gets a list of filenames with multiple versions,
    and returns the latest versjons of the files.
    
    Parameters
    ----------
    glob_list_path: list[str]
        List of strings that represents a filepath

    Returns
    -------
    list[str]
        List of strings with unique filepaths and its latest versions
    """
    return [sorted([file for file in glob_list_path if file.startswith(unique)])[-1]
            for unique in
            sorted(list(set([file[0] for file in [file.split('_v') for file in glob_list_path]])))]