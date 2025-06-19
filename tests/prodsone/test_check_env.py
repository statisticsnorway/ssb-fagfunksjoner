import os
from unittest import mock

import pytest
from dapla_auth_client.const import DaplaRegion

from fagfunksjoner.prodsone import check_env


def test_check_env_dapla():
    with mock.patch(
        "dapla_auth_client.AuthClient.get_dapla_region",
        return_value=DaplaRegion.DAPLA_LAB,
    ):
        assert check_env.check_env() == "DAPLA"


def test_check_env_prod():
    with mock.patch("dapla_auth_client.AuthClient.get_dapla_region", return_value=""):
        with mock.patch("os.path.isdir", return_value=True):
            assert check_env.check_env() == "PROD"


def test_check_env_unknown():
    with mock.patch(
        "dapla_auth_client.AuthClient.get_dapla_region", side_effect=AttributeError
    ):
        with mock.patch("os.path.isdir", return_value=False):
            assert check_env.check_env(raise_err=False) == "UNKNOWN"


def test_check_env_raises_error():
    with mock.patch(
        "dapla_auth_client.AuthClient.get_dapla_region", side_effect=AttributeError
    ):
        with mock.patch("os.path.isdir", return_value=False):
            try:
                check_env.check_env(raise_err=True)
            except OSError as e:
                assert str(e) == "Not on Dapla or in Prodsone, where are we dude?"


def test_linux_shortcuts():
    file_content = """export VAR1=value1
export VAR2=value2"""

    with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
        result = check_env.linux_shortcuts()
        assert result == {"VAR1": "value1", "VAR2": "value2"}


def test_linux_shortcuts_insert_environ():
    file_content = """export VAR1=value1
export VAR2=value2"""

    with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
        with mock.patch.dict(os.environ, {}, clear=True):
            result = check_env.linux_shortcuts(insert_environ=True)
            assert result == {"VAR1": "value1", "VAR2": "value2"}
            assert os.environ["VAR1"] == "value1"
            assert os.environ["VAR2"] == "value2"


def test_linux_shortcuts_invalid_format():
    file_content = "export VAR1=value1=value2"

    with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
        with pytest.raises(ValueError):
            check_env.linux_shortcuts()
