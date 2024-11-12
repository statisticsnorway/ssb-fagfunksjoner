import os
import pytest
import subprocess
from unittest.mock import patch

from fagfunksjoner.paths.user import find_email, find_user, verify_ssbmail


@pytest.fixture
def setup_env():
    """Fixture to set and reset environment variables."""
    os.environ["DAPLA_USER"] = "abc@ssb.no"
    os.environ["JUPYTERHUB_USER"] = "def@ssb.no"
    yield
    # Clean up environment variables after the test
    os.environ.pop("DAPLA_USER", None)
    os.environ.pop("JUPYTERHUB_USER", None)


def test_verify_ssbmail():
    assert verify_ssbmail("abc") == "abc@ssb.no"
    assert verify_ssbmail("abc@ssb.no") == "abc@ssb.no"
    assert verify_ssbmail("invalid_email") is None
    assert verify_ssbmail(None) is None


def test_find_email_dapla_user(setup_env):
    assert find_email() == "abc@ssb.no"
    assert find_user() == "abc"


def test_find_email_jupyterhub_user(setup_env):
    os.environ.pop("DAPLA_USER", None)  # Remove DAPLA_USER
    assert find_email() == "def@ssb.no"


def test_find_email_git_user(setup_env):
    os.environ.pop("DAPLA_USER", None)
    os.environ.pop("JUPYTERHUB_USER", None)
    with patch("subprocess.run") as mock_subprocess:
        mock_subprocess.return_value.stdout = "ghi"
        assert find_email() == "ghi@ssb.no"


def test_find_email_getpass_user(setup_env):
    os.environ.pop("DAPLA_USER", None)
    os.environ.pop("JUPYTERHUB_USER", None)
    with patch("subprocess.run") as mock_subprocess, patch("getpass.getuser") as mock_getuser:
        mock_subprocess.return_value.stdout = "invalid_email"
        mock_getuser.return_value = "jkl"
        assert find_email() == "jkl@ssb.no"


def test_find_email_raises_value_error(setup_env):
    os.environ.pop("DAPLA_USER", None)
    os.environ.pop("JUPYTERHUB_USER", None)
    with patch("subprocess.run") as mock_subprocess, patch("getpass.getuser") as mock_getuser:
        mock_subprocess.return_value.stdout = "invalid_email"
        mock_getuser.return_value = "invalid_user"
        with pytest.raises(ValueError, match="Cant find the users email or tbf in the system."):
            find_email()


def test_find_user(setup_env):
    assert find_user() == "abc"
    os.environ.pop("DAPLA_USER", None)
    assert find_user() == "def"
    os.environ.pop("JUPYTERHUB_USER", None)
    with patch("subprocess.run") as mock_subprocess:
        mock_subprocess.return_value.stdout = "ghi@ssb.no"
        assert find_user() == "ghi"