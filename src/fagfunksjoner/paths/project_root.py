"""This module lets you easily navigate to the root of your local project files,
and back from whence you came.
One of the main uses will be importing local functions in a notebook based project.
As notebooks run from the folder they are opened from, not root, and functions
usually will be .py files located in other folders than the notebooks."""

import os
from pathlib import Path

import toml

START_DIR = None


class ProjectRoot:
    """Contextmanager to import locally "with". As in:
    with ProjectRoot():
        from src.functions.local_functions import local_function

    So this class navigates back and forth using a single line/"instruction"
    """

    def __init__(self):
        self.path = find_root()
        self.workdir = Path(os.getcwd())

    def __enter__(self):
        navigate_root()

    @staticmethod
    def __exit__(exc_type, exc_value, traceback):
        return_to_work_dir()
        if exc_type is not None:
            print(traceback)
            raise exc_type(exc_value)

    @staticmethod
    def load_toml(config_file: str) -> dict:
        """Looks for a .toml file to load the contents from,
        in the current folder, the specified path, the project root.

        Parameters
        ----------
        config_file: str
            The path or filename of the config-file to load.

        Returns
        -------
        dict
            The contents of the toml-file

        Raises
        ------
        OSError
            If the file specified is not found in the current folder,
            the specified path, or the project root.
        """
        return load_toml(config_file)


def navigate_root() -> Path:
    """Changes the current working directory to the project root.
    Saves the folder it start from in the global variable (in this module) START_DIR

    Returns
    -------
    pathlib.Path
        The starting directory, where you are currently, as a pathlib Path.
        Changing the current working directory to root (different than returned)
        as a side-effect.
    """
    global START_DIR
    START_DIR = os.getcwd()
    os.chdir(find_root())
    return Path(START_DIR)


def find_root() -> Path:
    """Finds the root of the project, based on the hidden folder ".git",
    which you usually should have only in your project root.
    Changes the current working directory back and forth,
    but should end up in the original starting directory.

    Returns
    -------
    pathlib.Path
        The project root folder.
    """
    global START_DIR
    START_DIR = os.getcwd()
    for _ in range(len(Path(START_DIR).parents)):
        if ".git" in os.listdir():
            break
        os.chdir("../")
    else:
        os.chdir(START_DIR)
        raise OSError("Couldnt find .git navigating out from current folder.")
    project_root = os.getcwd()
    os.chdir(START_DIR)
    return Path(project_root)


def return_to_work_dir():
    """Navigates back to the last recorded START_DIR"""
    global START_DIR
    if START_DIR:
        os.chdir(START_DIR)
    else:
        print("START_DIR not set, assuming you never left the working dir")


def load_toml(config_file: str) -> dict:
    """Looks for a .toml file to load the contents from,
    in the current folder, the specified path, the project root.

    Parameters
    ----------
    config_file: str
        The path or filename of the config-file to load.

    Returns
    -------
    dict
        The contents of the toml-file

    Raises
    ------
    OSError
        If the file specified is not found in the current folder,
        the specified path, or the project root.
    """
    # Toml is in current folder
    if config_file in os.listdir():
        ...
    # User sent in complete path?
    elif os.path.isfile(config_file):
        ...
    # We found the config-file in the project_root
    elif os.path.isfile(Path(find_root()) / config_file):
        config_file = Path(find_root()) / config_file
    else:
        raise OSError(f"Cant find that config-file: {config_file}")
    return toml.load(config_file)
