from typing import Any, cast

import requests
from dapla_auth_client import AuthClient

DAPLA_CTRL_API_URI = "https://dapla-ctrl.intern.ssb.no/graphql/"


def get_from_dapla_team_api(query: str, unwrap_nodes: bool = True) -> dict[str, Any]:
    """Send a query to the Dapla Team API, test your queries at: https://dapla-ctrl.intern.ssb.no/api-docs, send them as a string here.

    Args:
        query: The full query as a string. Remember that line shifts are important in a graphql query.
        unwrap_nodes: If you dont want the nodes-levels to be stripped out of the result, set this to False.

    Returns:
        dict[str, Any]: The resulting nested dict that follows the response from the API.
    """
    result = requests.post(
        DAPLA_CTRL_API_URI,
        headers={
            "Authorization": "Bearer "
            + AuthClient.fetch_personal_token(audiences=["dapla-api"])
        },
        json={"query": str(query)},
    )
    result.raise_for_status()
    response_data = result.json()
    if unwrap_nodes:
        response_data = _unwrap_nodes(response_data)
    return cast(dict[str, Any], response_data)


def _unwrap_nodes(value: Any) -> Any:
    """Recursively replace GraphQL `nodes` wrappers with their contents.

    Args:
        value: Nested dictionaries, lists, or scalar values.

    Returns:
        The same structure with dictionaries of the form
        `{"nodes": [...]}` replaced by their node lists.
    """
    if isinstance(value, dict):
        if set(value) == {"nodes"}:
            return _unwrap_nodes(value["nodes"])

        return {key: _unwrap_nodes(item) for key, item in value.items()}

    if isinstance(value, list):
        return [_unwrap_nodes(item) for item in value]

    return value
