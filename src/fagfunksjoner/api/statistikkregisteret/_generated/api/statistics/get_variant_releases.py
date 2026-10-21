from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.get_variant_releases_response_200 import GetVariantReleasesResponse200
from ...types import UNSET, Response, Unset


def _get_kwargs(
    shortname: str,
    id: int,
    *,
    start: int | Unset = 0,
    count: int | Unset = 10,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["start"] = start

    params["count"] = count

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/statistics/{shortname}/variants/{id}/releases".format(
            shortname=quote(str(shortname), safe=""),
            id=quote(str(id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | GetVariantReleasesResponse200:
    if response.status_code == 200:
        response_200 = GetVariantReleasesResponse200.from_dict(response.json())

        return response_200

    response_default = Error.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | GetVariantReleasesResponse200]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    shortname: str,
    id: int,
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
) -> Response[Error | GetVariantReleasesResponse200]:
    """List all releases for one variant

    Args:
        shortname (str):
        id (int):
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetVariantReleasesResponse200]
    """

    kwargs = _get_kwargs(
        shortname=shortname,
        id=id,
        start=start,
        count=count,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    shortname: str,
    id: int,
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
) -> Error | GetVariantReleasesResponse200 | None:
    """List all releases for one variant

    Args:
        shortname (str):
        id (int):
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetVariantReleasesResponse200
    """

    return sync_detailed(
        shortname=shortname,
        id=id,
        client=client,
        start=start,
        count=count,
    ).parsed


async def asyncio_detailed(
    shortname: str,
    id: int,
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
) -> Response[Error | GetVariantReleasesResponse200]:
    """List all releases for one variant

    Args:
        shortname (str):
        id (int):
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetVariantReleasesResponse200]
    """

    kwargs = _get_kwargs(
        shortname=shortname,
        id=id,
        start=start,
        count=count,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    shortname: str,
    id: int,
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
) -> Error | GetVariantReleasesResponse200 | None:
    """List all releases for one variant

    Args:
        shortname (str):
        id (int):
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetVariantReleasesResponse200
    """

    return (
        await asyncio_detailed(
            shortname=shortname,
            id=id,
            client=client,
            start=start,
            count=count,
        )
    ).parsed
