from pytest_mock import MockerFixture

from fagfunksjoner.dapla.gsm import get_secret_version


def test_get_secret_version(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()

    mock_response = mocker.Mock()
    mock_response.payload.data = b"super-secret-value"

    mock_client.access_secret_version.return_value = mock_response

    mocker.patch(
        "fagfunksjoner.dapla.gsm.SecretManagerServiceClient",
        return_value=mock_client,
    )

    project_id = "tester-a92f"
    shortname = "supersecret"

    result = get_secret_version(project_id=project_id, shortname=shortname)

    assert result == "super-secret-value"

    mock_client.access_secret_version.assert_called_once_with(
        name="projects/tester-a92f/secrets/supersecret/versions/latest"
    )
