import datetime as dt
from collections.abc import Iterator
from unittest.mock import Mock

import httpx
import pytest

from fagfunksjoner.api import statistikkregisteret as reg
from fagfunksjoner.api.statistikkregisteret import _transport, _utilities
from fagfunksjoner.api.statistikkregisteret.models import (
    UNSET,
    ReleaseDetails,
    ReleaseListing,
    StatisticDetails,
)

NOW = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)


def statistic():
    return {
        "shortname": "kargrs",
        "name": "Karakterer og nasjonale prøver",
        "name_en": "Marks and national tests",
        "main_language": "nb",
        "division": {"code": "360", "name": "Utdanning"},
        "status": {"code": "A"},
        "first_released_at": "2002-01-01T00:00:00.000Z",
        "updated_at": "2026-05-01T06:00:00Z",
        "contacts": [
            {
                "name": "Example",
                "principalName": "example@ssb.no",
                "email": "fresh@ssb.no",
                "phone": "123",
            }
        ],
        "variants": [
            {
                "id": 8786,
                "level_of_detail": {
                    "name": "Karakterer ved avsluttet grunnskole",
                    "name_en": "Marks, lower secondary school",
                },
                "frequency": {"code": "A"},
                "revision": {"code": "I"},
                "cancelled": False,
            },
            {
                "id": 177998,
                "level_of_detail": {
                    "name": "Nasjonale prøver",
                    "name_en": "National tests",
                },
                "frequency": {"code": "A"},
                "revision": {"code": "I"},
                "cancelled": False,
            },
            {
                "id": 99,
                "level_of_detail": {"name": "Nasjonale prøver, cancelled"},
                "cancelled": True,
            },
        ],
    }


def release(identifier, timestamp, status="GODKJENT"):
    return {
        "id": identifier,
        "publish_time": timestamp,
        "approval_status": status,
        "period_from": "2026-01-01",
        "period_to": "2026-12-31",
    }


@pytest.fixture
def api(monkeypatch):
    calls = []
    routes = {}
    original = _transport.make_client
    reg.configure(base_url="https://statreg.test/api", page_size=2)
    monkeypatch.setattr(_utilities, "_now", lambda: NOW)

    def handle(request):
        calls.append(request)
        handler = routes.get(request.url.path)
        if handler is None:
            raise AssertionError(f"Unexpected request: {request.url}")
        payload = handler(request) if callable(handler) else handler
        if isinstance(payload, httpx.Response):
            return payload
        return httpx.Response(200, json=payload)

    def client(config):
        result = original(config)
        result._httpx_args["transport"] = httpx.MockTransport(handle)
        return result

    monkeypatch.setattr(_transport, "make_client", client)
    yield routes, calls
    reg.configure()


def register_iterator():
    with pytest.warns(DeprecationWarning, match="lazy iterator"):
        result = reg.get_statistics_register()
    assert isinstance(result, Iterator)
    return result


def paginated(field, items):
    def response(request):
        start = int(request.url.params.get("start", 0))
        count = int(request.url.params.get("count", 10))
        return {"total": len(items), field: items[start : start + count]}

    return response


def test_register_is_lazy_replays_partial_cache_and_isolates_mutation(api):
    routes, calls = api
    items = [
        {"id": i, "shortname": f"stat{i}", "division": {"code": "360"}}
        for i in range(5)
    ]
    routes["/api/statistics"] = paginated("statistics", items)
    first = register_iterator()
    assert not calls
    item = next(first)
    item["division"]["code"] = "changed"
    assert len(calls) == 1
    second = register_iterator()
    assert next(second)["division"]["code"] == "360"
    assert len(calls) == 1
    assert len(list(second)) == 4
    assert len(calls) == 3
    assert len(list(first)) == 4
    assert len(list(register_iterator())) == 5
    assert len(calls) == 3
    reg.clear_cache()
    list(register_iterator())
    assert len(calls) == 6


def test_configure_clears_cache_and_uses_token(api):
    routes, calls = api
    routes["/api/statistics"] = {"total": 0, "statistics": []}
    list(register_iterator())
    reg.configure(base_url="https://other.test/api", token="test-token", page_size=2)
    list(register_iterator())
    assert len(calls) == 2
    assert calls[-1].url.host == "other.test"
    assert calls[-1].headers["Authorization"] == "Bearer test-token"


def test_failed_register_page_is_not_cached(api):
    routes, calls = api
    routes["/api/statistics"] = httpx.Response(503, text="temporarily unavailable")
    with pytest.raises(httpx.HTTPStatusError):
        list(register_iterator())
    routes["/api/statistics"] = {"total": 0, "statistics": []}
    assert list(register_iterator()) == []
    assert len(calls) == 2


@pytest.mark.parametrize(
    "payload",
    [
        {"total": 4, "statistics": []},
        {"statistics": []},
        {"total": -1, "statistics": []},
    ],
)
def test_invalid_pagination_fails_instead_of_truncating_or_looping(api, payload):
    routes, _ = api
    routes["/api/statistics"] = payload
    with pytest.raises(RuntimeError, match="page"):
        list(register_iterator())


def test_shortcode_uses_direct_endpoint_generated_dates_and_fresh_contacts(api):
    routes, calls = api
    routes["/api/statistics/kargrs"] = statistic()
    result = reg.get_statistic_shortcode("kargrs")
    assert isinstance(result, StatisticDetails)
    assert result.first_released_at == dt.date(2002, 1, 1)
    assert result.updated_at.tzinfo is not None
    assert result.contacts[0].principal_name == "example@ssb.no"
    assert reg.get_contacts_shortcode("kargrs")[0].email == "fresh@ssb.no"
    routes["/api/statistics/kargrs"]["contacts"][0]["email"] = "new@ssb.no"
    assert reg.get_contacts_shortcode("kargrs")[0].email == "new@ssb.no"
    assert all(request.url.path == "/api/statistics/kargrs" for request in calls)


def test_missing_optional_fields_and_explicit_null(api):
    routes, _ = api
    routes["/api/statistics/kargrs"] = {
        "shortname": "kargrs",
        "first_released_at": None,
    }
    stat = reg.get_statistic_shortcode("kargrs")
    assert stat.first_released_at is None
    assert stat.contacts is UNSET
    assert reg.get_contacts_shortcode("kargrs") == []
    assert reg.get_variants_shortcode("kargrs") == []


def test_language_and_variant_selection(api):
    routes, _ = api
    routes["/api/statistics/kargrs"] = statistic()
    selected = reg.select_variant_shortcode("kargrs", terms=["NASJ", "prøv"])
    assert selected.id == 177998
    assert reg.get_variant_name(selected) == "Nasjonale prøver"
    assert (
        reg.select_variant_shortcode("kargrs", terms=["national"], language="en").id
        == selected.id
    )
    assert (
        reg.select_variant_shortcode(
            "kargrs", variant_id=8786, frequency="A", revision="I"
        ).id
        == 8786
    )
    assert len(reg.get_variants_shortcode("kargrs")) == 2
    assert len(reg.get_variants_shortcode("kargrs", include_cancelled=True)) == 3
    assert reg.get_statistic_name_shortcode("kargrs") == statistic()["name"]
    assert (
        reg.get_statistic_name_shortcode("kargrs", language="en")
        == statistic()["name_en"]
    )
    routes["/api/statistics/kargrs"]["name"] = ""
    assert reg.get_statistic_name_shortcode("kargrs") == statistic()["name_en"]
    with pytest.raises(ValueError, match="language"):
        reg.select_variant_shortcode("kargrs", language="fr")


@pytest.mark.parametrize(
    "filters", [{}, {"terms": ["unknown"]}, {"terms": ["nasj"], "variant_id": 8786}]
)
def test_variant_ambiguity_includes_available_choices(api, filters):
    routes, _ = api
    routes["/api/statistics/kargrs"] = statistic()
    with pytest.raises(reg.VariantSelectionError, match="Available variants"):
        reg.select_variant_shortcode("kargrs", **filters)


def test_invalid_term_string_rejected_before_request(api):
    with pytest.raises(ValueError, match="sequence"):
        reg.select_variant_shortcode("kargrs", terms="nasj")


def release_endpoint(items):
    def response(request):
        filtered = list(items)
        params = request.url.params
        if "approval_status" in params:
            filtered = [
                item
                for item in filtered
                if item["approval_status"] == params["approval_status"]
            ]
        for key, compare in [
            ("publish_time_before", lambda a, b: a <= b),
            ("publish_time_after", lambda a, b: a > b),
        ]:
            if key in params:
                boundary = dt.datetime.fromisoformat(params[key])
                filtered = [
                    item
                    for item in filtered
                    if compare(
                        dt.datetime.fromisoformat(
                            item["publish_time"].replace("Z", "+00:00")
                        ),
                        boundary,
                    )
                ]
        filtered.sort(
            key=lambda item: item["publish_time"],
            reverse=params.get("sort", "").startswith("-"),
        )
        return paginated("releases", filtered)(request)

    return response


def test_sorted_date_lookup_fetches_one_result_and_next_differs_from_latest(api):
    routes, calls = api
    routes["/api/releases"] = release_endpoint(
        [
            release(1, "2026-01-01T08:00:00Z"),
            release(2, "2026-07-01T08:00:00Z"),
            release(3, "2026-09-01T08:00:00Z"),
            release(4, "2027-01-01T08:00:00Z", "FORSLAG"),
        ]
    )
    assert reg.get_last_publish_date_shortcode("kargrs").month == 9
    assert calls[-1].url.params["count"] == "1"
    assert calls[-1].url.params["sort"] == "-publish_time"
    assert reg.get_next_publish_date_shortcode("kargrs").month == 7
    assert calls[-1].url.params["sort"] == "publish_time"
    assert (
        reg.get_last_publish_date_shortcode("kargrs", include_future=False).month == 1
    )
    assert (
        reg.get_last_publish_date_shortcode("kargrs", approval_status=None).year == 2027
    )
    assert len(calls) == 4


def test_variant_date_scans_only_selected_variant_and_all_pages(api):
    routes, calls = api
    routes["/api/statistics/kargrs"] = statistic()
    routes["/api/statistics/kargrs/variants/177998/releases"] = paginated(
        "releases",
        [
            release(1, "2026-09-01T08:00:00Z"),
            release(2, "2026-01-01T08:00:00Z"),
            release(3, "2026-07-01T08:00:00Z"),
            release(4, "2027-07-01T08:00:00Z", "FORSLAG"),
        ],
    )
    assert (
        reg.get_next_publish_date_shortcode("kargrs", terms=["nasj", "prøv"]).month == 7
    )
    assert len(calls) == 3
    assert reg.get_last_publish_date_shortcode("kargrs", variant_id=177998).month == 9
    assert (
        reg.get_last_publish_date_shortcode(
            "kargrs", variant_id=177998, include_future=False
        ).month
        == 1
    )
    assert (
        reg.get_last_publish_date_shortcode(
            "kargrs", variant_id=177998, approval_status=None
        ).year
        == 2027
    )
    assert not any(request.url.path == "/api/releases" for request in calls)


def test_lazy_releases_optional_detail_and_future_filter(api):
    routes, calls = api
    routes["/api/releases"] = release_endpoint(
        [release(1, "2026-01-01T08:00:00Z"), release(2, "2026-09-01T08:00:00Z")]
    )
    routes["/api/releases/1"] = {
        **release(1, "2026-01-01T08:00:00Z"),
        "variant": {"id": 8786},
        "cancelled": False,
    }
    iterator = reg.iter_releases_shortcode("kargrs", include_future=False)
    assert not calls
    result = list(iterator)
    assert len(result) == 1
    assert isinstance(result[0], ReleaseListing)
    assert isinstance(result[0].period_from, dt.date)
    detailed = list(
        reg.iter_releases_shortcode("kargrs", include_future=False, details=True)
    )
    assert isinstance(detailed[0], ReleaseDetails)
    assert detailed[0].variant.id == 8786
    assert detailed[0].cancelled is False


def test_empty_dates_and_negative_time_until_fallback(api):
    routes, _ = api
    routes["/api/releases"] = release_endpoint([])
    assert reg.get_next_publish_date_shortcode("kargrs") is None
    assert reg.get_last_publish_date_shortcode("kargrs") is None
    assert reg.time_until_publishing("kargrs") is None
    routes["/api/releases"] = release_endpoint([release(1, "2026-01-01T08:00:00Z")])
    assert reg.time_until_publishing("kargrs") < dt.timedelta(0)


@pytest.mark.parametrize("remaining", [None, dt.timedelta(days=-1)])
def test_future_publishing_error_and_warning(monkeypatch, remaining):
    monkeypatch.setattr(
        _utilities, "time_until_publishing", lambda shortname: remaining
    )
    log = Mock()
    monkeypatch.setattr(_utilities, "logger", log)
    with pytest.raises(reg.FuturePublishingError):
        reg.raise_on_missing_future_publish("kargrs")
    assert reg.raise_on_missing_future_publish("kargrs", raise_error=False) == remaining
    log.warning.assert_called_once()
    log.info.assert_not_called()


def test_future_publishing_info(monkeypatch):
    monkeypatch.setattr(
        _utilities, "time_until_publishing", lambda shortname: dt.timedelta(days=10)
    )
    assert reg.raise_on_missing_future_publish("kargrs") == dt.timedelta(days=10)


def test_legacy_adapters_warn_and_enrichment_does_not_mutate(api):
    routes, _ = api
    routes["/api/statistics/kargrs"] = statistic()
    routes["/api/releases"] = release_endpoint([])
    routes["/api/releases/1"] = release(1, "2026-01-01T08:00:00Z")
    with pytest.warns(DeprecationWarning, match="shortcode"):
        assert isinstance(reg.single_stat("kargrs"), StatisticDetails)
    with pytest.warns(DeprecationWarning), pytest.raises(ValueError, match="shortcode"):
        reg.single_stat()
    with pytest.warns(DeprecationWarning, match="lazy iterator"):
        assert list(reg.find_publishings("kargrs", False)) == []
    with pytest.warns(DeprecationWarning, match="ReleaseDetails"):
        assert reg.specific_publishing("1").id == 1
    original = {"shortname": "kargrs"}
    with pytest.warns(DeprecationWarning, match="new API fields"):
        result = reg.get_singles_publishings(original, get_publishings=False)
    assert original == {"shortname": "kargrs"}
    assert isinstance(result["product_info"], StatisticDetails)


def test_section_filter_and_substring_search(api):
    routes, _ = api
    routes["/api/statistics"] = paginated(
        "statistics",
        [
            {
                "id": 1,
                "shortname": "kargrs",
                "division": {"code": "360"},
                "status": {"code": "A"},
            },
            {
                "id": 2,
                "shortname": "other",
                "division": {"code": "360"},
                "status": {"code": "UT"},
            },
            {"id": 3, "shortname": "elsewhere", "division": {"code": "100"}},
        ],
    )
    routes["/api/statistics/kargrs"] = statistic()
    with pytest.warns(DeprecationWarning):
        assert len(reg.sections_publishings(360, get_publishings=False)) == 1
    with pytest.warns(DeprecationWarning):
        found = reg.find_stat_shortcode(
            "karg", get_singles=False, get_publishings=False
        )
    assert found[0]["shortname"] == "kargrs"


def test_global_contacts_new_shape(api):
    routes, _ = api
    routes["/api/contacts"] = [{"name": "Example", "principalName": "example@ssb.no"}]
    with pytest.warns(DeprecationWarning):
        assert reg.get_contacts()[0].principal_name == "example@ssb.no"


def test_http_error_handles_non_json_and_timeout(api):
    routes, _ = api
    routes["/api/statistics/kargrs"] = httpx.Response(401, text="login required")
    with pytest.raises(httpx.HTTPStatusError) as caught:
        reg.get_statistic_shortcode("kargrs")
    assert caught.value.response.status_code == 401

    def timeout(request):
        raise httpx.ReadTimeout("timed out", request=request)

    routes["/api/statistics/kargrs"] = timeout
    with pytest.raises(httpx.ReadTimeout):
        reg.get_contacts_shortcode("kargrs")


@pytest.mark.parametrize(
    "options", [{"timeout": 0}, {"page_size": 0}, {"base_url": "invalid"}]
)
def test_invalid_configuration(options):
    with pytest.raises(ValueError):
        reg.configure(**options)


def test_naive_release_timestamp_is_rejected(api):
    routes, _ = api
    routes["/api/releases"] = {
        "total": 1,
        "releases": [release(1, "2026-07-01T08:00:00")],
    }
    with pytest.raises(ValueError, match="timezone"):
        reg.get_last_publish_date_shortcode("kargrs")


@pytest.mark.parametrize("value", ["4922", " 4922 ", "", " "])
def test_shortcode_requires_non_numeric_nonempty_input(value):
    with pytest.raises(ValueError, match="shortcode"):
        reg.get_statistic_shortcode(value)
