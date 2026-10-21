"""Handwritten shortcode utilities over the generated Statreg read client."""

from __future__ import annotations

import datetime as dt
import warnings
from collections.abc import Iterator, Sequence
from copy import deepcopy
from functools import cache
from typing import Any

from fagfunksjoner.fagfunksjoner_logger import logger

from . import _transport
from ._generated.api.contacts import get_contacts as contacts_api
from ._generated.api.releases import get_release, get_releases
from ._generated.api.statistics import (
    get_statistic,
    get_statistics,
    get_variant_releases,
)
from ._generated.models import (
    GetReleasesResponse200,
    GetStatisticsResponse200,
    GetVariantReleasesResponse200,
)
from ._generated.types import UNSET, Unset
from .models import (
    Contact,
    ReleaseDetails,
    ReleaseListing,
    StatisticDetails,
    StatisticDetailsContactsItem,
    Variant,
)


class FuturePublishingError(Exception):
    """Missing expected publication at a future time."""


class VariantSelectionError(ValueError):
    """Variant filters matched zero or several current variants."""


def _deprecated(message: str) -> None:
    warnings.warn(message, DeprecationWarning, stacklevel=3)


def _shortcode(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or value.strip().isdigit():
        raise ValueError(
            "Use a statistic shortcode (for example 'hugjen'), not a numeric statistic ID."
        )
    return value.strip()


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Statreg returned publish_time without a timezone")
    return value


def _page_items(page: Any, field: str, start: int) -> tuple[list[Any], bool]:
    items = getattr(page, field)
    total = page.total
    if not isinstance(items, list) or not isinstance(total, int) or total < 0:
        raise RuntimeError(
            f"Invalid Statreg page: expected {field} and a nonnegative total"
        )
    if not items and start < total:
        raise RuntimeError(
            f"Statreg returned an empty {field} page before total was reached"
        )
    return items, start + len(items) >= total


@cache
def _register_page(config: _transport.Settings, start: int) -> GetStatisticsResponse200:
    return _transport.request(
        get_statistics.sync_detailed,
        GetStatisticsResponse200,
        config=config,
        start=start,
        count=config.page_size,
        sort="id",
    )


def clear_cache() -> None:
    """Discard all register pages; subsequent iterations fetch fresh pages.

    Clear the cache between complete iterations to refresh the register. Existing
    iterators retain their position and must be recreated for a full refresh.
    Detail/contact/release lookups are never cached.
    """
    _register_page.cache_clear()


def get_statistics_register() -> Iterator[dict[str, Any]]:
    """Lazily iterate register dictionaries, reusing fetched pages in memory.

    Each call returns a new iterator. Requests happen during iteration; failures
    can therefore occur on next(). Use list(...) to materialize all entries.
    Only successfully fetched pages are cached, until clear_cache/configure.
    This endpoint includes all statistics, not only released products.
    """
    _deprecated(
        "get_statistics_register() now returns a lazy iterator, not a list. "
        "Use list(...) for indexing/len(); fetched pages are cached until clear_cache(). "
        "Fields follow the new API (shortname, integer id, division); all statistics are included."
    )
    config = _transport.settings()

    def iterate() -> Iterator[dict[str, Any]]:
        start = 0
        while True:
            page = _register_page(config, start)
            items, done = _page_items(page, "statistics", start)
            for item in items:
                yield deepcopy(item.to_dict())
            if done:
                return
            start += len(items)

    return iterate()


def get_statistic_shortcode(shortcode: str) -> StatisticDetails:
    """Fetch current metadata directly by shortcode, without scanning the register."""
    return _transport.request(
        get_statistic.sync_detailed, StatisticDetails, shortname=_shortcode(shortcode)
    )


def get_contacts_shortcode(shortcode: str) -> list[StatisticDetailsContactsItem]:
    """Fetch current contacts, including email and phone when supplied by Statreg."""
    contacts = get_statistic_shortcode(shortcode).contacts
    return [] if isinstance(contacts, Unset) else contacts


def get_contacts() -> list[Contact]:
    """Fetch the global contact directory (name/principal_name only)."""
    _deprecated(
        "get_contacts() returns generated Contact models with name/principal_name. Use get_contacts_shortcode() for email/phone; legacy contact fields are unavailable."
    )
    contacts = _transport.request(contacts_api.sync_detailed, list)
    return [contact for contact in contacts if isinstance(contact, Contact)]


def _text(name: Any, name_en: Any, language: str | None) -> str | None:
    if language not in (None, "nb", "nn", "en"):
        raise ValueError("language must be None (main language), 'nb', 'nn', or 'en'")
    preferred, fallback = (name_en, name) if language == "en" else (name, name_en)
    for value in (preferred, fallback):
        if isinstance(value, str) and value.strip():
            return value
    return None


def get_statistic_name_shortcode(
    shortcode: str, *, language: str | None = None
) -> str | None:
    """Return main-language text by default, falling back to the available name.

    Explicit en prefers name_en. nb/nn select the single main-language name;
    the API does not supply separate Bokmaal and Nynorsk translations.
    """
    statistic = get_statistic_shortcode(shortcode)
    return _text(statistic.name, statistic.name_en, language)


def get_variant_name(variant: Variant, *, language: str | None = None) -> str | None:
    """Return a variant's level-of-detail name, preferring its main-language text."""
    if isinstance(variant.level_of_detail, Unset):
        return None
    return _text(
        variant.level_of_detail.name, variant.level_of_detail.name_en, language
    )


def get_variants_shortcode(
    shortcode: str, *, include_cancelled: bool = False
) -> list[Variant]:
    """Fetch current variants, excluding explicitly cancelled variants by default."""
    variants = get_statistic_shortcode(shortcode).variants
    if isinstance(variants, Unset):
        return []
    return [v for v in variants if include_cancelled or v.cancelled is not True]


def select_variant_shortcode(
    shortcode: str,
    *,
    terms: Sequence[str] | None = None,
    variant_id: int | None = None,
    language: str | None = None,
    frequency: str | None = None,
    revision: str | None = None,
) -> Variant:
    """Require exactly one current variant matching all supplied filters.

    Terms are case-insensitive substrings of level_of_detail.name (or name_en).
    Terms are resolved afresh; callers need not persist migrated variant IDs.
    Both terms and an ID may be supplied to validate the same variant.
    """
    if isinstance(terms, str) or (
        terms is not None and any(not t.strip() for t in terms)
    ):
        raise ValueError("terms must be a sequence of nonempty search strings")
    _text(None, None, language)
    candidates = get_variants_shortcode(shortcode)
    found = []
    for variant in candidates:
        name = (get_variant_name(variant, language=language) or "").casefold()
        if variant_id is not None and variant.id != variant_id:
            continue
        if terms is not None and not all(term.casefold() in name for term in terms):
            continue
        if frequency is not None and (
            isinstance(variant.frequency, Unset) or variant.frequency.code != frequency
        ):
            continue
        if revision is not None and (
            isinstance(variant.revision, Unset) or variant.revision.code != revision
        ):
            continue
        found.append(variant)
    if len(found) != 1:
        choices = [(v.id, get_variant_name(v, language=language)) for v in candidates]
        raise VariantSelectionError(
            f"Expected one variant for {shortcode!r}; matched {len(found)}. Available variants: {choices!r}"
        )
    return found[0]


def _variant_id(
    shortcode: str,
    terms: Sequence[str] | None,
    variant_id: int | None,
    language: str | None,
) -> int | None:
    if terms is not None:
        selected = select_variant_shortcode(
            shortcode, terms=terms, variant_id=variant_id, language=language
        )
        if not isinstance(selected.id, int):
            raise ValueError("Selected variant has no ID in the API response")
        return selected.id
    if variant_id is not None and variant_id <= 0:
        raise ValueError("variant_id must be positive")
    return variant_id


def _release_pages(
    config: _transport.Settings,
    shortcode: str,
    variant_id: int | None,
    *,
    sort: str = "-publish_time",
    count: int | None = None,
    approval_status: str | None = "GODKJENT",
    before: dt.datetime | None = None,
    after: dt.datetime | None = None,
) -> Iterator[ReleaseListing]:
    start = 0
    while True:
        page: GetReleasesResponse200 | GetVariantReleasesResponse200
        if variant_id is None:
            page = _transport.request(
                get_releases.sync_detailed,
                GetReleasesResponse200,
                config=config,
                start=start,
                count=count or config.page_size,
                sort=sort,
                shortname=shortcode,
                approval_status=(
                    approval_status if approval_status is not None else UNSET
                ),
                publish_time_before=before if before is not None else UNSET,
                publish_time_after=after if after is not None else UNSET,
            )
        else:
            page = _transport.request(
                get_variant_releases.sync_detailed,
                GetVariantReleasesResponse200,
                config=config,
                shortname=shortcode,
                id=variant_id,
                start=start,
                count=count or config.page_size,
            )
        items, done = _page_items(page, "releases", start)
        for release in items:
            if (
                approval_status is not None
                and release.approval_status != approval_status
            ):
                continue
            if before is not None or after is not None:
                if not isinstance(release.publish_time, dt.datetime):
                    continue
                timestamp = _aware(release.publish_time)
                if before is not None and timestamp > before:
                    continue
                if after is not None and timestamp <= after:
                    continue
            yield release
        if done:
            return
        start += len(items)


def iter_releases_shortcode(
    shortcode: str,
    *,
    terms: Sequence[str] | None = None,
    variant_id: int | None = None,
    language: str | None = None,
    include_future: bool = True,
    approval_status: str | None = "GODKJENT",
    details: bool = False,
) -> Iterator[ReleaseListing | ReleaseDetails]:
    """Lazily fetch release models, optionally selecting one variant by terms/ID.

    Approved releases are returned by default; approval_status=None includes all
    statuses. details=True adds one detail request per release. Only detail
    responses expose cancellation/archive flags. No release results are cached.
    """
    shortcode = _shortcode(shortcode)
    selected_id = _variant_id(shortcode, terms, variant_id, language)
    config = _transport.settings()
    for release in _release_pages(
        config,
        shortcode,
        selected_id,
        approval_status=approval_status,
        before=None if include_future else _now(),
    ):
        if details:
            if not isinstance(release.id, int):
                raise RuntimeError("Release listing has no ID")
            yield _transport.request(
                get_release.sync_detailed,
                ReleaseDetails,
                config=config,
                id=str(release.id),
            )
        else:
            yield release


def _publish_date(
    shortcode: str,
    *,
    terms: Sequence[str] | None,
    variant_id: int | None,
    language: str | None,
    include_future: bool,
    next_date: bool,
    approval_status: str | None,
) -> dt.datetime | None:
    shortcode = _shortcode(shortcode)
    selected_id = _variant_id(shortcode, terms, variant_id, language)
    now = _now()
    releases = _release_pages(
        _transport.settings(),
        shortcode,
        selected_id,
        sort="publish_time" if next_date else "-publish_time",
        count=1 if selected_id is None else None,
        approval_status=approval_status,
        before=now if not include_future else None,
        after=now if next_date else None,
    )
    dates = []
    for release in releases:
        if isinstance(release.publish_time, dt.datetime):
            timestamp = _aware(release.publish_time)
            if selected_id is None:
                return timestamp
            dates.append(timestamp)
    if not dates:
        return None
    return min(dates) if next_date else max(dates)


def get_last_publish_date_shortcode(
    shortcode: str,
    *,
    terms: Sequence[str] | None = None,
    variant_id: int | None = None,
    language: str | None = None,
    include_future: bool = True,
    approval_status: str | None = "GODKJENT",
) -> dt.datetime | None:
    """Return the maximum publication timestamp, including future dates by default.

    include_future=False restricts results to now/past. A shortcode-only lookup
    requests the first sorted result; variant lookups scan that variant's pages
    because its endpoint has no documented sorting parameter.
    """
    return _publish_date(
        shortcode,
        terms=terms,
        variant_id=variant_id,
        language=language,
        include_future=include_future,
        next_date=False,
        approval_status=approval_status,
    )


def get_next_publish_date_shortcode(
    shortcode: str,
    *,
    terms: Sequence[str] | None = None,
    variant_id: int | None = None,
    language: str | None = None,
    approval_status: str | None = "GODKJENT",
) -> dt.datetime | None:
    """Return the earliest future publication timestamp, or None if none is planned."""
    return _publish_date(
        shortcode,
        terms=terms,
        variant_id=variant_id,
        language=language,
        include_future=True,
        next_date=True,
        approval_status=approval_status,
    )


def single_stat(stat_id: str = "4922") -> StatisticDetails:
    """Fetch statistic details; the legacy stat_id parameter now requires a shortcode."""
    _deprecated(
        "single_stat(stat_id=...) now requires a shortcode and returns StatisticDetails. Use get_statistic_shortcode('hugjen'); numeric statistic IDs are unsupported."
    )
    return get_statistic_shortcode(stat_id)


def specific_publishing(publish_id: str = "162143") -> ReleaseDetails:
    """Fetch a release by its release ID (which is distinct from a variant ID)."""
    _deprecated(
        "specific_publishing() returns ReleaseDetails: use publish_time, variant.id and cancelled; legacy PublishingSpecifics fields/types have changed."
    )
    return _transport.request(
        get_release.sync_detailed, ReleaseDetails, id=str(publish_id)
    )


def find_publishings(
    shortname: str = "trosamf",
    get_publishing_specifics: bool = True,
    *,
    include_future: bool = True,
) -> Iterator[ReleaseListing | ReleaseDetails]:
    """Return a lazy release iterator; optional details cause a request per release."""
    _deprecated(
        "find_publishings() now returns a lazy iterator of generated release models, without .publishings/.amount/.specifics. Prefer iter_releases_shortcode(); details=True fetches each release separately."
    )
    return iter_releases_shortcode(
        shortname, include_future=include_future, details=get_publishing_specifics
    )


def find_latest_publishing(
    shortname: str = "trosamf",
    *,
    include_future: bool = True,
) -> ReleaseListing | None:
    """Return the release with the greatest timestamp, possibly in the future."""
    _deprecated(
        "find_latest_publishing() returns ReleaseListing with publish_time, not StatisticPublishingShort.specifics.time. Use get_next_publish_date_shortcode() for the next planned date."
    )
    return next(
        _release_pages(
            _transport.settings(),
            _shortcode(shortname),
            None,
            count=1,
            before=None if include_future else _now(),
        ),
        None,
    )


def time_until_publishing(shortname: str = "trosamf") -> dt.timedelta | None:
    """Return time until the nearest planned release, falling back to the latest past one."""
    next_date = get_next_publish_date_shortcode(shortname)
    if next_date is None:
        next_date = get_last_publish_date_shortcode(shortname, include_future=False)
    return None if next_date is None else next_date - _now()


def raise_on_missing_future_publish(
    shortname: str, raise_error: bool = True
) -> dt.timedelta | None:
    """Raise FuturePublishingError or log a warning when no future release is found."""
    remaining = time_until_publishing(shortname)
    if remaining is None:
        message = f"Cant find any publishing times for {shortname}. Are you using the correct shortname?"
    elif remaining < dt.timedelta(0):
        message = f"You haven't added a next publishing to the register yet? {remaining.days} days since last publishing?"
    else:
        logger.info(f"Publishing in {remaining.days} days, according to register.")
        return remaining
    if raise_error:
        raise FuturePublishingError(message)
    logger.warning(message)
    return remaining


def get_singles_publishings(
    stat: dict[str, Any],
    shortcode_or_id: str = "trosamf",
    get_singles: bool = True,
    get_publishings: bool = True,
    get_publishing_specifics: bool = True,
) -> dict[str, Any]:
    """Enrich a copy of a statistic dictionary using the new API's fields/models."""
    _deprecated(
        "get_singles_publishings() uses new API fields, generated models, and lazy release iterators; shortcode_or_id requires a shortcode."
    )
    result = deepcopy(stat)
    shortcode = _shortcode(str(stat.get("shortname", shortcode_or_id)))
    if get_singles:
        result["product_info"] = get_statistic_shortcode(shortcode)
    if get_publishings:
        result["publishings"] = iter_releases_shortcode(
            shortcode, details=get_publishing_specifics
        )
    return result


def find_stat_shortcode(
    shortcode_or_id: str = "trosamf",
    get_singles: bool = True,
    get_publishings: bool = True,
    get_publishing_specifics: bool = True,
) -> list[dict[str, Any]]:
    """Retain legacy substring search; prefer direct helpers for exact shortcodes."""
    _deprecated(
        "find_stat_shortcode() searches shortcodes only and returns dictionaries with new API fields/generated models and lazy release iterators. Prefer get_statistic_shortcode() for a direct lookup."
    )
    shortcode = _shortcode(shortcode_or_id)
    # Substring matching requires the register; exact helpers avoid this scan.
    config = _transport.settings()
    start = 0
    results = []
    while True:
        items, done = _page_items(_register_page(config, start), "statistics", start)
        for stat in items:
            if isinstance(stat.shortname, str) and shortcode in stat.shortname:
                results.append(
                    get_singles_publishings(
                        stat.to_dict(),
                        stat.shortname,
                        get_singles,
                        get_publishings,
                        get_publishing_specifics,
                    )
                )
        if done:
            return results
        start += len(items)


def sections_publishings(
    section_code: str | int,
    include_ceased: bool = False,
    get_publishings: bool = True,
    get_publishing_specifics: bool = True,
) -> list[StatisticDetails]:
    """Fetch a section's detailed statistics, with optional lazy releases as extra data.

    The new spec has no division filter; this helper scans cached register pages
    and fetches details for matching statistics. Prefer direct shortcode helpers.
    """
    _deprecated(
        "sections_publishings() returns generated StatisticDetails (division/status.code). Optional releases are a lazy iterator in stat['publishings']; the API has no section filter."
    )
    config = _transport.settings()
    start = 0
    results = []
    while True:
        items, done = _page_items(_register_page(config, start), "statistics", start)
        for stat in items:
            if isinstance(stat.division, Unset) or stat.division.code != str(
                section_code
            ):
                continue
            if (
                not include_ceased
                and not isinstance(stat.status, Unset)
                and stat.status.code in {"IA", "UT", "SA"}
            ):
                continue
            if not isinstance(stat.shortname, str):
                continue
            details = get_statistic_shortcode(stat.shortname)
            if get_publishings:
                details["publishings"] = iter_releases_shortcode(
                    stat.shortname, details=get_publishing_specifics
                )
            results.append(details)
        if done:
            return results
        start += len(items)
