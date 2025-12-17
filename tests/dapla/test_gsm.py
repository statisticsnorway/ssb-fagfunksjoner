from pytest_mock import MockerFixture

from fagfunksjoner.dapla.gsm import get_secret_version


def test_get_secret_version(mocker: MockerFixture) -> None:
    mock_smclient = mocker.patch("fagfunksjoner.dapla.gsm.SecretManagerServiceClient")

    project_id = "tester-a92f"
    shortname = "supersecret"

    get_secret_version(project_id=project_id, shortname=shortname)
    mock_smclient.assert_called_once()
