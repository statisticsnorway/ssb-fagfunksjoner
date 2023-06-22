import os
import toml
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


def load_config_toml(config_file: str) -> dict:
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


class ProjectRoot:
    """Contextmanager to import locally "with"."""

    @staticmethod
    def __enter__():
        navigate_root()

    @staticmethod
    def __exit__():
        return_to_work_dir()
