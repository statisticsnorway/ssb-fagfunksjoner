import datetime
from http import HTTPStatus
from typing import Any

import httpx

from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.get_releases_response_200 import GetReleasesResponse200
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    start: int | Unset = 0,
    count: int | Unset = 10,
    sort: str | Unset = UNSET,
    shortname: str | Unset = UNSET,
    approval_status: str | Unset = UNSET,
    publish_time_after: datetime.datetime | Unset = UNSET,
    publish_time_before: datetime.datetime | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["start"] = start

    params["count"] = count

    params["sort"] = sort

    params["shortname"] = shortname

    params["approval_status"] = approval_status

    json_publish_time_after: str | Unset = UNSET
    if not isinstance(publish_time_after, Unset):
        json_publish_time_after = publish_time_after.isoformat()
    params["publish_time_after"] = json_publish_time_after

    json_publish_time_before: str | Unset = UNSET
    if not isinstance(publish_time_before, Unset):
        json_publish_time_before = publish_time_before.isoformat()
    params["publish_time_before"] = json_publish_time_before

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/releases",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | GetReleasesResponse200:
    if response.status_code == 200:
        response_200 = GetReleasesResponse200.from_dict(response.json())

        return response_200

    response_default = Error.from_dict(response.json())

    return response_default


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | GetReleasesResponse200]:
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
    sort: str | Unset = UNSET,
    shortname: str | Unset = UNSET,
    approval_status: str | Unset = UNSET,
    publish_time_after: datetime.datetime | Unset = UNSET,
    publish_time_before: datetime.datetime | Unset = UNSET,
) -> Response[Error | GetReleasesResponse200]:
    """List all releases. Filter and sorting can be specified using query params

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        sort (str | Unset):
        shortname (str | Unset):
        approval_status (str | Unset):
        publish_time_after (datetime.datetime | Unset):
        publish_time_before (datetime.datetime | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetReleasesResponse200]
    """

    kwargs = _get_kwargs(
        start=start,
        count=count,
        sort=sort,
        shortname=shortname,
        approval_status=approval_status,
        publish_time_after=publish_time_after,
        publish_time_before=publish_time_before,
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
    sort: str | Unset = UNSET,
    shortname: str | Unset = UNSET,
    approval_status: str | Unset = UNSET,
    publish_time_after: datetime.datetime | Unset = UNSET,
    publish_time_before: datetime.datetime | Unset = UNSET,
) -> Error | GetReleasesResponse200 | None:
    """List all releases. Filter and sorting can be specified using query params

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        sort (str | Unset):
        shortname (str | Unset):
        approval_status (str | Unset):
        publish_time_after (datetime.datetime | Unset):
        publish_time_before (datetime.datetime | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetReleasesResponse200
    """

    return sync_detailed(
        client=client,
        start=start,
        count=count,
        sort=sort,
        shortname=shortname,
        approval_status=approval_status,
        publish_time_after=publish_time_after,
        publish_time_before=publish_time_before,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
    sort: str | Unset = UNSET,
    shortname: str | Unset = UNSET,
    approval_status: str | Unset = UNSET,
    publish_time_after: datetime.datetime | Unset = UNSET,
    publish_time_before: datetime.datetime | Unset = UNSET,
) -> Response[Error | GetReleasesResponse200]:
    """List all releases. Filter and sorting can be specified using query params

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        sort (str | Unset):
        shortname (str | Unset):
        approval_status (str | Unset):
        publish_time_after (datetime.datetime | Unset):
        publish_time_before (datetime.datetime | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetReleasesResponse200]
    """

    kwargs = _get_kwargs(
        start=start,
        count=count,
        sort=sort,
        shortname=shortname,
        approval_status=approval_status,
        publish_time_after=publish_time_after,
        publish_time_before=publish_time_before,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    start: int | Unset = 0,
    count: int | Unset = 10,
    sort: str | Unset = UNSET,
    shortname: str | Unset = UNSET,
    approval_status: str | Unset = UNSET,
    publish_time_after: datetime.datetime | Unset = UNSET,
    publish_time_before: datetime.datetime | Unset = UNSET,
) -> Error | GetReleasesResponse200 | None:
    """List all releases. Filter and sorting can be specified using query params

    Args:
        start (int | Unset):  Default: 0.
        count (int | Unset):  Default: 10.
        sort (str | Unset):
        shortname (str | Unset):
        approval_status (str | Unset):
        publish_time_after (datetime.datetime | Unset):
        publish_time_before (datetime.datetime | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetReleasesResponse200
    """

    return (
        await asyncio_detailed(
            client=client,
            start=start,
            count=count,
            sort=sort,
            shortname=shortname,
            approval_status=approval_status,
            publish_time_after=publish_time_after,
            publish_time_before=publish_time_before,
        )
    ).parsed
