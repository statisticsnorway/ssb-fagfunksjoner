import os
from unittest import mock

import pytest

from fagfunksjoner.prodsone.check_env import check_env, linux_shortcuts


def test_check_env_dapla():
    with mock.patch.dict(os.environ, {"JUPYTER_IMAGE_SPEC": "jupyterlab-dapla"}):
        assert check_env() == "DAPLA"


def test_check_env_prod():
    with mock.patch("os.path.isdir", return_value=True):
        with mock.patch.dict(os.environ, {}, clear=True):
            assert check_env() == "PROD"


def test_check_env_error():
    with mock.patch("os.path.isdir", return_value=False):
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(OSError):
                check_env()


def test_linux_shortcuts():
    file_content = """export VAR1=value1
export VAR2=value2"""

    with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
        result = linux_shortcuts()
        assert result == {"VAR1": "value1", "VAR2": "value2"}


def test_linux_shortcuts_insert_environ():
    file_content = """export VAR1=value1
export VAR2=value2"""

    with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
        with mock.patch.dict(os.environ, {}, clear=True):
            result = linux_shortcuts(insert_environ=True)
            assert result == {"VAR1": "value1", "VAR2": "value2"}
            assert os.environ["VAR1"] == "value1"
            assert os.environ["VAR2"] == "value2"


def test_linux_shortcuts_invalid_format():
    file_content = "export VAR1=value1=value2"

    with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
        with pytest.raises(ValueError):
            linux_shortcuts()
