"""This module lets you easily navigate to the root of your local project files.

One of the main uses will be importing local functions in a notebook based project. 
As notebooks run from the folder they are opened from, not root, and functions usually will be .py files located in other folders than the notebooks."""

import os
from pathlib import Path
from types import TracebackType
from typing import Any

import toml

from fagfunksjoner.fagfunksjoner_logger import logger

START_DIR = None


class ProjectRoot:
    """Contextmanager to import locally "with".

    As in:
    .. code-block:: python

        with ProjectRoot():
            from src.functions.local_functions import local_function


    So this class navigates back and forth using a single line/"instruction"
    """

    def __init__(self) -> None:
        """Initialize the projectroot by finding the correct folder.

        And navigating back to, and storing the starting folder.
        """
        self.path = find_root()
        self.workdir = Path(os.getcwd())

    def __enter__(self) -> None:
        """Entering the context manager navigates to the project root."""
        navigate_root()

    @staticmethod
    def __exit__(
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Exiting the project root navigates back to the current folder.

        THEN raises any errors.
        """
        return_to_work_dir()
        if exc_type is not None:
            logger.warning(traceback)
            raise exc_type(exc_value)
        return True

    @staticmethod
    def load_toml(config_file: str) -> dict[Any, Any]:
        """Looks for a .toml file to load the contents from.

        Looks in the current folder, the specified path, the project root.

        Args:
            config_file (str): The path or filename of the config-file to load.

        Returns:
            dict[Any]: The contents of the toml-file.
        """
        return load_toml(config_file)


def navigate_root() -> Path:
    """Changes the current working directory to the project root.

    Saves the folder it start from in the global variable (in this module) START_DIR

    Returns:
        Path: The starting directory, where you are currently, as a pathlib Path.
            Changing the current working directory to root (different than returned)
            as a side-effect.
    """
    global START_DIR
    START_DIR = os.getcwd()
    os.chdir(find_root())
    return Path(START_DIR)


def find_root() -> Path:
    """Finds the root of the project, based on the hidden folder ".git".

    Which you usually should have only in your project root.
    Changes the current working directory back and forth,
    but should end up in the original starting directory.

    Returns:
        Path: The project root folder.

    Raises:
        OSError: If the file specified is not found in the current folder,
            the specified path, or the project root.
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


def return_to_work_dir() -> None:
    """Navigate back to the last recorded START_DIR."""
    global START_DIR
    if START_DIR:
        os.chdir(START_DIR)
    else:
        logger.info("START_DIR not set, assuming you never left the working dir")


def load_toml(config_file: str) -> dict[Any, Any]:
    """Look for a .toml file to load the contents from.

    Looks in the current folder, the specified path, the project root.

    Args:
        config_file (str): The path or filename of the config-file to load.

    Returns:
        dict[Any]: The contents of the toml-file

    Raises:
        OSError: If the file specified is not found in the current folder,
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
        config_file = str(Path(find_root()) / config_file)
    else:
        raise OSError(f"Cant find that config-file: {config_file}")
    return toml.load(config_file)
