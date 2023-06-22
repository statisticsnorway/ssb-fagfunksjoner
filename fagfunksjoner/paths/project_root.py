import os
from pathlib import Path


START_DIR = None

def navigate_root() -> Path:
    if not START_DIR:
        START_DIR = os.getcwd()
    os.chdir(find_root())
    return Path(original_dir)


def find_root() -> Path:
    if not START_DIR:
        START_DIR = os.getcwd()
    for _ in range(40):
        if ".git" in os.listdir():
            break
            os.chdir("../")
    root_dir = os.getcwd()
    os.chdir(orignal_dir)
    return Path(START_DIR)


def return_to_work_dir():
    if START_DIR:
        os.chdir(START_DIR)
    else:
        print("START_DIR not set, assuming you never left the working dir")
        