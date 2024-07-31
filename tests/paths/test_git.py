import pytest
from unittest import mock

from fagfunksjoner.paths.git import name_from_gitconfig


def test_name_from_gitconfig_found():
    mock_gitconfig_content = [
        "[user]\n",
        "name = John Doe\n",
    ]

    with mock.patch("builtins.open", mock.mock_open(read_data="".join(mock_gitconfig_content))):
        with mock.patch("os.getcwd", return_value="/home/user/project"):
            with mock.patch("os.listdir", side_effect=[[], [], [".gitconfig"]]):
                with mock.patch("os.chdir"):
                    assert name_from_gitconfig() == "John Doe"


def test_name_from_gitconfig_not_found():
    with mock.patch("os.getcwd", return_value="/home/user/project"):
        with mock.patch("os.listdir", side_effect=[[] for _ in range(40)]):
            with mock.patch("os.chdir"):
                with pytest.raises(FileNotFoundError) as excinfo:
                    name_from_gitconfig()
                assert "Couldn't find .gitconfig" in str(excinfo.value)


def test_name_from_gitconfig_correct_directory_revert():
    mock_gitconfig_content = [
        "[user]\n",
        "name = John Doe\n",
    ]

    with mock.patch("builtins.open", mock.mock_open(read_data="".join(mock_gitconfig_content))):
        with mock.patch("os.getcwd", return_value="/home/user/project") as mock_getcwd:
            with mock.patch("os.listdir", side_effect=[[], [], [".gitconfig"]]):
                with mock.patch("os.chdir") as mock_chdir:
                    name_from_gitconfig()
                    # Ensure os.chdir is called back to the original directory
                    mock_chdir.assert_any_call(mock_getcwd.return_value)