"""Code that uses things from git-files."""

import os


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
