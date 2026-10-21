from http import HTTPStatus
from typing import Any

import httpx

from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.get_statistics_response_200 import GetStatisticsResponse200
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    start: int | Unset = 0,
    count: int | Unset = 10,
    shortname: str | Unset = UNSET,
    contact: str | Unset = UNSET,
    sort: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["start"] = start

    params["count"] = count

    params["shortname"] = shortname

    params["contact"] = contact

    params["sort"] = sort

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/statistics",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | GetStatisticsResponse200:
    if response.status_code == 200:
        response_200 = GetStatisticsResponse200.from_dict(response.json())

        return response_200

    response_default = Error.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | GetStatisticsResponse200]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
    shortname: str | Unset = UNSET,
    contact: str | Unset = UNSET,
    sort: str | Unset = UNSET,
) -> Response[Error | GetStatisticsResponse200]:
    """List of all statistics

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        shortname (str | Unset):
        contact (str | Unset):
        sort (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetStatisticsResponse200]
    """

    kwargs = _get_kwargs(
        start=start,
        count=count,
        shortname=shortname,
        contact=contact,
        sort=sort,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
    shortname: str | Unset = UNSET,
    contact: str | Unset = UNSET,
    sort: str | Unset = UNSET,
) -> Error | GetStatisticsResponse200 | None:
    """List of all statistics

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        shortname (str | Unset):
        contact (str | Unset):
        sort (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetStatisticsResponse200
    """

    return sync_detailed(
        client=client,
        start=start,
        count=count,
        shortname=shortname,
        contact=contact,
        sort=sort,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
    shortname: str | Unset = UNSET,
    contact: str | Unset = UNSET,
    sort: str | Unset = UNSET,
) -> Response[Error | GetStatisticsResponse200]:
    """List of all statistics

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        shortname (str | Unset):
        contact (str | Unset):
        sort (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetStatisticsResponse200]
    """

    kwargs = _get_kwargs(
        start=start,
        count=count,
        shortname=shortname,
        contact=contact,
        sort=sort,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
    shortname: str | Unset = UNSET,
    contact: str | Unset = UNSET,
    sort: str | Unset = UNSET,
) -> Error | GetStatisticsResponse200 | None:
    """List of all statistics

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        shortname (str | Unset):
        contact (str | Unset):
        sort (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetStatisticsResponse200
    """

    return (
        await asyncio_detailed(
            client=client,
            start=start,
            count=count,
            shortname=shortname,
            contact=contact,
            sort=sort,
        )
    ).parsed
