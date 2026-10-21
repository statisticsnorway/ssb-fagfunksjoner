"""Configuration and HTTP transport for the Statreg convenience functions."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

import httpx

from ._generated.client import Client
from ._generated.types import Response

T = TypeVar("T")
DEFAULT_BASE_URL = "https://statistikkregisteret.intern.test.ssb.no/api"


@dataclass(frozen=True)
class Settings:
    """Immutable request settings, also used to partition cached register pages."""

    base_url: str = DEFAULT_BASE_URL
    timeout: float = 30.0
    page_size: int = 100
    token: str | None = field(default=None, repr=False)


_settings = Settings(
    base_url=os.environ.get("STATREG_BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
    token=os.environ.get("STATREG_TOKEN"),
)


def settings() -> Settings:
    """Return the current configuration."""
    return _settings


def _raise_http_error(response: httpx.Response) -> None:
    response.read()
    response.raise_for_status()


def make_client(config: Settings) -> Client:
    """Construct a generated client with timeouts and standard HTTP errors."""
    headers = {"Accept": "application/json"}
    if config.token:
        headers["Authorization"] = f"Bearer {config.token}"
    return Client(
        base_url=config.base_url,
        headers=headers,
        timeout=httpx.Timeout(config.timeout),
        httpx_args={"event_hooks": {"response": [_raise_http_error]}},
    )


def request(
    operation: Callable[..., Response[Any]],
    expected: type[T],
    *,
    config: Settings | None = None,
    **params: Any,
) -> T:
    """Execute a generated operation and require its documented success model."""
    with make_client(config or settings()) as client:
        response = operation(client=client, **params)
    if response.status_code != 200 or not isinstance(response.parsed, expected):
        raise RuntimeError(
            f"Unexpected Statreg response ({response.status_code}): {response.parsed!r}"
        )
    return response.parsed


def configure(
    *,
    base_url: str | None = None,
    timeout: float = 30.0,
    page_size: int = 100,
    token: str | None = None,
) -> None:
    """Configure the test/production server and clear cached register pages.

    Omitted base_url and token use STATREG_BASE_URL and STATREG_TOKEN. The
    fallback server is the internal test environment. TLS verification stays on.
    Each call replaces the previous configuration. No network request is made.
    """
    if timeout <= 0 or page_size <= 0:
        raise ValueError("timeout and page_size must be positive")
    url = (base_url or os.environ.get("STATREG_BASE_URL", DEFAULT_BASE_URL)).rstrip("/")
    if not url.startswith(("https://", "http://")):
        raise ValueError("base_url must be an HTTP(S) URL including the /api prefix")
    global _settings
    _settings = Settings(
        url, timeout, page_size, token or os.environ.get("STATREG_TOKEN")
    )
    from ._utilities import clear_cache

    clear_cache()
