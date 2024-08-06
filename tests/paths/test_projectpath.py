import os

from fagfunksjoner.paths.project_root import ProjectRoot


def test_ProjectRoot_context_manager() -> None:
    try:
        test_folder = "test_folder_from_ProjectRoot_test"
        os.makedirs(test_folder)
        os.chdir(test_folder)
        first_path = os.getcwd()
        with ProjectRoot():
            second_path = os.getcwd()
        third_path = os.getcwd()
        os.chdir("../")
    finally:
        os.rmdir(os.path.join(first_path))

    assert first_path == third_path
    assert first_path != second_path
