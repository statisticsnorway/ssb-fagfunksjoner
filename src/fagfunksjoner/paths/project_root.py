"""This module lets you easily navigate to the root of your local project files.

One of the main uses will be importing local functions in a notebook based project.
As notebooks run from the folder they are opened from, not root, and functions usually will be .py files located in other folders than the notebooks.
"""

import inspect
import os
from pathlib import Path
import sys
from types import TracebackType
from typing import Any

import toml

from fagfunksjoner.fagfunksjoner_logger import logger


class ProjectRoot:
    """Contextmanager to import locally "with".

    As in::

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
        os.chdir(self.path)
        sys.path.append(str(self.path))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Exiting the project root navigates back to the current folder.

        THEN raises any errors.
        """
        os.chdir(self.workdir)
        sys.path.pop(sys.path.index(str(self.path)))
        if exc_type is not None:
            logger.warning(traceback)
            raise exc_type(exc_value)
        return True

    @staticmethod
    def load_toml(config_file: str) -> dict[Any, Any]:
        """Looks for a .toml file to load the contents from.

        Looks in the current folder, the specified path, the project root.

        Args:
            config_file: The path or filename of the config-file to load.

        Returns:
            dict[Any]: The contents of the toml-file.
        """
        return load_toml(config_file)


def get_exec_path() -> Path:
    """Get the python path being executed.

    Will not work with Jupyter notebooks since there is no __file__ attribute.
    """
    frame = inspect.currentframe()
    # navigate to topmost frame
    while frame:
        prev_frame = frame.f_back
        if not prev_frame:
            break
        frame = prev_frame
    return Path(frame.f_locals["__file__"])


def find_root() -> Path:
    """Finds the root of the project, based on the hidden folder ".git".

    Which you usually should have only in your project root.

    Does not change the working directory.

    Returns:
        Path: The project root folder.

    Raises:
        OSError: If the file specified is not found in the current folder,
            the specified path, or the project root.
    """
    try:
        file_name = get_exec_path()
        wd = file_name.parent
    except KeyError:
        file_name = Path(os.getcwd())
        wd = file_name

    while True:
        if ".git" in os.listdir(wd):
            return wd
        if len(wd.parts) == 1:
            raise OSError(f"Couldnt find .git navigating out from {file_name}")
        wd = wd.parent


def load_toml(config_file: str) -> dict[Any, Any]:
    """Look for a .toml file to load the contents from.

    Looks in the current folder, the specified path, the project root.

    Args:
        config_file: The path or filename of the config-file to load.

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
