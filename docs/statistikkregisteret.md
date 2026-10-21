# Statistikkregisteret

The read client defaults to the internal test environment:
`https://statistikkregisteret.intern.test.ssb.no/api`.
You need network access to that environment. No requests are made on import.

```python
from fagfunksjoner.api import statistikkregisteret as reg

reg.configure(
    base_url="https://statistikkregisteret.intern.test.ssb.no/api",
    timeout=30,
    page_size=100,
)

statistic = reg.get_statistic_shortcode("hugjen")
contacts = reg.get_contacts_shortcode("hugjen")
next_date = reg.get_next_publish_date_shortcode("hugjen")
name = reg.get_statistic_name_shortcode("hugjen")
```

`STATREG_BASE_URL` and optional `STATREG_TOKEN` also configure the client.
`configure()` replaces the previous configuration, reads omitted URL/token
values from those environment variables, and clears register caches. The spec
allows these read endpoints without a bearer token. A token can be supplied
with `configure(token=...)` if the deployment requires one; obtain it through
your environment's normal authentication flow. TLS verification remains enabled.
HTTP failures raise `httpx.HTTPStatusError`; timeouts raise `httpx.TimeoutException`.

## Lazy register and caching

```python
from itertools import islice

# No request until iteration starts; pages are cached as they are fetched.
first_five = list(islice(reg.get_statistics_register(), 5))
# Replays fetched pages; requests further pages only when needed.
all_statistics = list(reg.get_statistics_register())
reg.clear_cache()
```

Each call returns a fresh iterator, never a cached generator. Successfully
fetched pages remain in memory until `clear_cache()` or `configure()`. Iteration
returns independent dictionaries, so editing one cannot corrupt cached pages.
Cache memory grows with pages consumed. Clear between complete iterations and
recreate iterators to refresh the whole register. This is not a server-side
snapshot: data can change between requests for uncached pages.

`get_statistics_register()` emits a `DeprecationWarning`: it previously returned
a list. Materialize it for indexing or `len()`. Network errors can occur during
iteration. The new listing includes all statistics, including upcoming products.
Exact shortcode helpers avoid scanning the register, and detail/contact/release
lookups are not cached so they can reflect current information.

## Variants and publication dates

```python
variant = reg.select_variant_shortcode("kargrs", terms=["nasj", "prøv"])
variant_name = reg.get_variant_name(variant)
next_date = reg.get_next_publish_date_shortcode("kargrs", variant_id=variant.id)

# Equivalent selection resolved against current variants on each call:
last_date = reg.get_last_publish_date_shortcode(
    "kargrs", terms=["avslutt", "grunnskol"], include_future=False,
)
```

Terms are case-insensitive substrings of the variant's `level_of_detail.name`.
All terms must match. You may also filter `select_variant_shortcode()` by
`frequency`, `revision`, or `variant_id`; filters combine. Zero/multiple matches
raise `VariantSelectionError` with available IDs and names. Cancelled variants
are excluded from selection. `get_variants_shortcode(include_cancelled=True)`
can inspect them. Explicit IDs avoid the statistic detail lookup for date/release
helpers; term searches resolve the current ID each time.

Observed test data associates the same national-tests variant ID with releases
in 2024 and 2025. This does not guarantee that IDs survive future edits/migrations.
Prefer terms when persisting a selection across environments.

| Helper | Meaning |
| --- | --- |
| `get_next_publish_date_shortcode()` | Earliest timestamp strictly after now, or `None` |
| `get_last_publish_date_shortcode()` | Greatest timestamp, including future dates by default |
| `get_last_publish_date_shortcode(include_future=False)` | Greatest timestamp at/before now |
| `time_until_publishing()` | Time until nearest future release; falls back to latest past release for a negative result |

Dates are timezone-aware `datetime` objects; no matches returns `None`. Date-only
fields such as `period_from` are `date` objects. Approved (`GODKJENT`) releases
are selected by default. Set `approval_status=None` to include proposals and
other statuses, which may be useful for planning work. Release listings do not
expose cancellation/archive flags; these helpers do not promise to exclude
cancelled individual releases. Request details to inspect those flags.

A shortcode-only date lookup asks for one sorted release. Variant endpoints do
not document sorting/time/status filters, so date helpers scan all pages for
that variant and apply filters locally. They avoid fetching each release detail.

```python
for release in reg.iter_releases_shortcode("kargrs", terms=["nasj", "prøv"]):
    print(release.publish_time, release.period_from, release.period_to)
```

`iter_releases_shortcode()` is lazy and accepts `include_future=True` (default),
`approval_status`, `terms`, `variant_id`, and `language`. `details=True` adds a
request per release and yields generated `ReleaseDetails` models instead of
`ReleaseListing`. Shortcode lists use descending publication order; variant
lists retain server order.

Text helpers default to the main-language `name`, falling back to `name_en` when
empty. `language="en"` prefers English and can also select English variant terms.
There is only one main-language name (described by `main_language`, nb/nn), not
separate translations for Bokmaal and Nynorsk. Contacts and dates have no
language parameter.

## Existing entrypoints

Imports remain `from fagfunksjoner.api import statistikkregisteret as reg`.
The module is now a package with handwritten utilities and a generated client.
There is no `MultiplePublishings` wrapper.

| Previous usage | Replacement |
| --- | --- |
| `single_stat(stat_id)` | `get_statistic_shortcode(shortcode)`; numeric statistic IDs/default `4922` raise a clear error |
| `find_publishings(...).publishings` | Iterate `iter_releases_shortcode(...)` directly |
| `publishing.specifics.time` | `release.publish_time` |
| `specific_publishing(id)` | Still accepts a release ID; returns `ReleaseDetails` |
| `find_latest_publishing()` | Returns `ReleaseListing` with the greatest date, including future by default |
| `shortName`, string IDs | `shortname`, integer IDs |
| `owningsection`, expanded status text | `division`, `status.code` |
| Nested multilingual contact names | `contact.name`, `contact.principal_name` |

Changed legacy entrypoints emit caller-located `DeprecationWarning`s. Enable
these with Python's `-Wd` flag. `find_stat_shortcode()` retains substring matching
and enrichment flags but returns lists with new API fields; it scans cached
register pages. `get_singles_publishings()` returns a copy and enriches with
generated models/lazy release iterators. `sections_publishings()` also scans the
register because no division filter is documented; it returns `StatisticDetails`
with optional lazy releases at `stat["publishings"]`.

`find_publishings(get_publishing_specifics=True)` retains its legacy default,
which adds a detail request per release when iterated. Prefer the new iterator's
cheaper default. `FuturePublishingError` and `raise_on_missing_future_publish()`
remain available.

Models are re-exported from `reg.models`. Missing optional fields are
`reg.models.UNSET`, distinct from explicit `None`. `model.to_dict()` returns
API field names and JSON-compatible dates. Model Python attributes use snake
case (for example `principal_name`), while dictionaries use `principalName`.

## Regenerate the client

Runtime users need neither the YAML nor the generator. Generated Python is
version-controlled and included in the wheel. The local source YAML is ignored.
With Python 3.11+:

```console
poetry install --with codegen
poetry run python tools/generate_statreg_client.py
```

Supply `--spec PATH` for another local spec and `--upstream-revision COMMIT` when
its upstream revision is known. Generated `provenance.json` records the input
SHA-256 and generator version; unknown upstream revisions remain null.
The command replaces only `_generated`, leaving handwritten files untouched.

The six GET operations used by this MVP are generated; write operations are
excluded. Preprocessing normalizes legacy nullable keywords, flattens object
`allOf` while retaining sibling fields, and gives shared status a schema name.
Pinned templates accept observed timestamps in date-only fields (returning the
date component) and parse UTC `Z` timestamps on Python 3.10. Generated `Self`
uses `typing_extensions` for that same runtime compatibility. The original
YAML is not modified. Review generated diffs whenever the source spec changes.

Offline verification:

```console
poetry run pytest tests/api/test_statistikkregisteret.py
```

These tests use an in-memory HTTP transport and need no API connectivity.
