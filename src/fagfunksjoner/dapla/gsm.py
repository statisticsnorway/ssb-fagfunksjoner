from google.cloud.secretmanager import SecretManagerServiceClient


def get_secret_version(
    project_id: str, shortname: str, version_id: str = "latest"
) -> str:
    """Access the payload for a given secret version.

    The user's google credentials are used to authorize that the user has permission
    to access the secret_id.

    Args:
        project_id: ID of the Google Cloud project where the secret is stored.
        shortname: Name (not full path) of the secret in Secret Manager.
        version_id: The version of the secret to access. Defaults to 'latest'.

    Returns:
        The payload of the secret version as a UTF-8 decoded string.
    """
    client = SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{shortname}/versions/{version_id}"
    response = client.access_secret_version(name=secret_name)
    return str(response.payload.data.decode("UTF-8"))
