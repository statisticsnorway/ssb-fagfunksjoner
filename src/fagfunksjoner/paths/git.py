"""Code that uses things from git-files."""

import os


def name_from_gitconfig() -> str:
    curr_dir = os.getcwd()
    for _ in range(40):
        if ".gitconfig" in os.listdir():
            break
        os.chdir("../")
    else:
        err = """Couldn't find .gitconfig,
        have you run ssb-gitconfig.py from the terminal?"""
        raise ImportError(err)
    with open(".gitconfig") as gitconfig:
        gitconf = gitconfig.readlines()
    for line in gitconf:
        line = line.replace("\t", "").replace("\n", "").strip()
        if line.startswith("name ="):
            name = " ".join(line.split(" ")[2:])
    os.chdir(curr_dir)
    return name
