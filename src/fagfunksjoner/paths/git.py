"""Code that uses things from git-files."""

import os
from pathlib import Path


def name_from_gitconfig() -> str:
    """Find the username from the git config in the current system.

    Returns:
        str: The found Username
    Raises:
        FileNotFoundError: if the .gitconfig file is not found by navigating out through the storage.
    """
    curr_dir = os.getcwd()
    for _ in range(40):
        if ".gitconfig" in os.listdir():
            break
        os.chdir("../")
    else:
        err = """Couldn't find .gitconfig,
        have you run ssb-gitconfig.py from the terminal?"""
        raise FileNotFoundError(err)
    with open(".gitconfig") as gitconfig:
        gitconf = gitconfig.readlines()
    for line in gitconf:
        line = line.replace("\t", "").replace("\n", "").strip()
        if line.startswith("name ="):
            name = " ".join(line.split(" ")[2:])
    os.chdir(curr_dir)
    return name


def repo_root_dir(directory: Path | str | None = None) -> Path:
    """Find the root directory of a git repo, searching upwards from a given path.

    Args:
        directory: The path to search from, defaults to the current working directory.
            The directory can be of type string or of type pathlib.Path.

    Returns:
        Path to the git repo's root directory.

    Raises:
        RuntimeError: If no .git directory is found when searching upwards.

    Example:
    --------
    >>> from fagfunksjoner.paths.git import repo_root_dir
    >>> import tomli
    >>>
    >>> config_file = repo_root_dir() / "pyproject.toml"
    >>> with open(config_file, mode="rb") as fp:
    >>>     config = tomli.load(fp)
    """
    if directory is None:
        directory = Path.cwd()

    if isinstance(directory, str):
        directory = Path(directory)

    while directory / ".git" not in directory.iterdir():
        if directory == Path("/"):
            raise RuntimeError(f"The directory {directory} is not in a git repo.")
        directory = directory.parent
    return directory
