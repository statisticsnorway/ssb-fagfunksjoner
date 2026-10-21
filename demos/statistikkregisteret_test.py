# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#   kernelspec:
#     display_name: Python 3 (ssb-fagfunksjoner)
#     language: python
#     name: python3
# ---


# %% [markdown]
# # Test Statistikkregisteret: hugjen and vgogjen
#
# Select this repository's Python environment as the notebook kernel. Run the
# cells in order. Requests are read-only and use the internal test API; you need
# network access to it. No OpenAPI generator is needed to run this notebook.
#
# The configuration cell controls shortcodes, page size, and optional extra
# requests. Outputs may contain contact details; clear outputs before sharing.
# A missing next publication date is returned as `None`.

# %%
import os
import sys
import warnings
from itertools import islice
from pathlib import Path
from time import perf_counter

import pandas as pd
from IPython.display import display

# Prefer this checkout when starting Jupyter from the repository or demos/.
for directory in (Path.cwd(), *Path.cwd().parents):
    if (directory / "src" / "fagfunksjoner").is_dir():
        sys.path.insert(0, str(directory / "src"))
        break

from fagfunksjoner.api import statistikkregisteret as reg

warnings.simplefilter("default", DeprecationWarning)
print("Python:", sys.executable)
print("Statreg package:", reg.__file__)

# %% [markdown]
# ## Configuration
#
# Change `BASE_URL` to switch servers. `STATREG_TOKEN`, if set in your kernel's
# environment, is picked up automatically without printing the token.
# The two optional switches below enable additional requests in later cells.

# %%
BASE_URL = os.environ.get(
    "STATREG_BASE_URL", "https://statistikkregisteret.intern.test.ssb.no/api"
)
SHORTCODES = ["hugjen", "vgogjen"]
PAGE_SIZE = 10
RELEASE_SAMPLE_SIZE = 5
CHECK_VARIANT_DATES = False
INSPECT_RELEASE_DETAILS = False

reg.configure(base_url=BASE_URL, timeout=30, page_size=PAGE_SIZE)
print("Server:", BASE_URL)

# %% [markdown]
# ## Current statistic metadata
#
# These direct lookups do not scan the register or use its cache. Inspect the
# full generated model with `statistics["hugjen"]` or its API-shaped dictionary
# with `statistics["hugjen"].to_dict()`. Missing optional model fields are
# `reg.models.UNSET`, distinct from explicit `None`.

# %%
statistics = {
    shortcode: reg.get_statistic_shortcode(shortcode) for shortcode in SHORTCODES
}
metadata_rows = []
for shortcode, statistic in statistics.items():
    data = statistic.to_dict()
    metadata_rows.append(
        {
            "shortcode": shortcode,
            "name": data.get("name") or data.get("name_en"),
            "name_en": data.get("name_en"),
            "main_language": data.get("main_language"),
            "status": data.get("status", {}).get("code"),
            "division": data.get("division", {}).get("code"),
            "updated_at": data.get("updated_at"),
            "first_released_at": data.get("first_released_at"),
        }
    )
display(pd.DataFrame(metadata_rows))

# %% [markdown]
# ## Fresh contacts by shortcode
#
# `get_contacts_shortcode()` makes a fresh statistic-detail request on every call.
# It includes email and phone when provided, unlike the global contact directory.

# %%
contacts_by_shortcode = {
    shortcode: reg.get_contacts_shortcode(shortcode) for shortcode in SHORTCODES
}
contact_rows = [
    {"shortcode": shortcode, **contact.to_dict()}
    for shortcode, contacts in contacts_by_shortcode.items()
    for contact in contacts
]
display(pd.DataFrame(contact_rows))

# %% [markdown]
# ## Next planned and latest publication dates
#
# The next planned date is the **earliest future** timestamp. The last date is
# the **greatest** timestamp, including future dates by default.
# `include_future=False` limits the last-date lookup to now/past.
#
# These helpers select approved (`GODKJENT`) releases. To investigate proposals
# as well, repeat a call with `approval_status=None`. Cancellation/archive flags
# are only exposed by release details, not by these date lookups.

# %%
date_rows = []
for shortcode in SHORTCODES:
    date_rows.append(
        {
            "shortcode": shortcode,
            "next_planned": reg.get_next_publish_date_shortcode(shortcode),
            "latest_including_future": reg.get_last_publish_date_shortcode(shortcode),
            "latest_past": reg.get_last_publish_date_shortcode(
                shortcode, include_future=False
            ),
        }
    )
display(pd.DataFrame(date_rows))

# %% [markdown]
# ## Inspect variants and their names
#
# Variant names are in `level_of_detail`. The table includes cancelled variants
# for inspection; normal selection excludes them. These are the IDs used by the
# variant-specific release endpoint, distinct from release IDs.

# %%
variants_by_shortcode = {
    shortcode: reg.get_variants_shortcode(shortcode, include_cancelled=True)
    for shortcode in SHORTCODES
}
variant_rows = []
for shortcode, variants in variants_by_shortcode.items():
    for variant in variants:
        data = variant.to_dict()
        variant_rows.append(
            {
                "shortcode": shortcode,
                "variant_id": data.get("id"),
                "name": reg.get_variant_name(variant),
                "name_en": reg.get_variant_name(variant, language="en"),
                "frequency": data.get("frequency", {}).get("code"),
                "revision": data.get("revision", {}).get("code"),
                "cancelled": data.get("cancelled"),
            }
        )
display(pd.DataFrame(variant_rows))

# %% [markdown]
# ## Select a variant by terms or ID
#
# Edit `SELECT_SHORTCODE`, then either supply `SELECT_TERMS` from the names above
# or copy an ID into `SELECT_VARIANT_ID`. Leaving both unset skips selection if
# there are multiple active variants. Zero or multiple matches produce an
# explanatory error rather than silently choosing a variant.

# %%
SELECT_SHORTCODE = "hugjen"
SELECT_TERMS = None  # Example: ["word", "another word"] from the variant name.
SELECT_VARIANT_ID = None  # Alternatively copy an ID from the table above.
SELECT_LANGUAGE = None  # Main language by default; use "en" for English terms.

active_variants = [
    variant
    for variant in variants_by_shortcode[SELECT_SHORTCODE]
    if variant.cancelled is not True
]
selected_variant = None
if (
    SELECT_TERMS is not None
    or SELECT_VARIANT_ID is not None
    or len(active_variants) == 1
):
    selected_variant = reg.select_variant_shortcode(
        SELECT_SHORTCODE,
        terms=SELECT_TERMS,
        variant_id=SELECT_VARIANT_ID,
        language=SELECT_LANGUAGE,
    )
    display(pd.DataFrame([selected_variant.to_dict()]))
else:
    print("Set SELECT_TERMS or SELECT_VARIANT_ID to choose one active variant.")

# %% [markdown]
# ## Lazy release samples
#
# Taking a sample fetches only the pages needed. Lists include future dates by
# default; set `include_future=False` to view only past releases. No individual
# release-detail requests are made in this cell.

# %%
release_samples = {
    shortcode: list(islice(reg.iter_releases_shortcode(shortcode), RELEASE_SAMPLE_SIZE))
    for shortcode in SHORTCODES
}
for shortcode, releases in release_samples.items():
    print(shortcode)
    display(pd.json_normalize([release.to_dict() for release in releases]))

# %% [markdown]
# ## Optional: compare publication dates per variant
#
# Set `CHECK_VARIANT_DATES=True` in the configuration cell and rerun this cell.
# Variant endpoints have no documented sorting/time filters, so each date lookup
# may fetch all pages for that variant. Start with the shortcode-level dates above
# when speed matters.

# %%
if CHECK_VARIANT_DATES:
    variant_dates = []
    for shortcode, variants in variants_by_shortcode.items():
        for variant in variants:
            if variant.cancelled is True or variant.id is reg.models.UNSET:
                continue
            variant_dates.append(
                {
                    "shortcode": shortcode,
                    "variant_id": variant.id,
                    "name": reg.get_variant_name(variant),
                    "next_planned": reg.get_next_publish_date_shortcode(
                        shortcode, variant_id=variant.id
                    ),
                    "latest_including_future": reg.get_last_publish_date_shortcode(
                        shortcode, variant_id=variant.id
                    ),
                }
            )
    display(pd.DataFrame(variant_dates))
else:
    print("Skipped: enable CHECK_VARIANT_DATES to fetch per-variant dates.")

# %% [markdown]
# ## Optional: verify the variant IDs attached to releases
#
# Set `INSPECT_RELEASE_DETAILS=True` to fetch details for up to three sampled
# releases per shortcode. Compare their IDs to the variant table. This adds a
# request for each detail; it can demonstrate ID reuse across releases, but not
# guarantee that an ID will survive future migration or edits.

# %%
if INSPECT_RELEASE_DETAILS:
    detail_rows = []
    for shortcode, releases in release_samples.items():
        for release in releases[:3]:
            # This legacy-named helper returns the generated ReleaseDetails model.
            detail = reg.specific_publishing(str(release.id)).to_dict()
            detail_rows.append(
                {
                    "shortcode": shortcode,
                    "release_id": detail.get("id"),
                    "variant_id": detail.get("variant", {}).get("id"),
                    "publish_time": detail.get("publish_time"),
                    "period_from": detail.get("period_from"),
                    "period_to": detail.get("period_to"),
                    "cancelled": detail.get("cancelled"),
                    "archived": detail.get("archived"),
                }
            )
    display(pd.DataFrame(detail_rows))
else:
    print("Skipped: enable INSPECT_RELEASE_DETAILS to check release-to-variant IDs.")

# %% [markdown]
# ## Lazy register cache replay
#
# The first partial iteration fetches only enough pages for five entries. The
# second starts from the beginning and reuses the pages already in memory.
# A deprecation warning explains that the result is now an iterator.
# The register includes all statistics, including upcoming products.
#
# Elapsed times are illustrative and depend on the environment. A full register
# iteration is deliberately omitted. Detail and contact lookups remain uncached.

# %%
reg.clear_cache()
started = perf_counter()
first_pass = list(islice(reg.get_statistics_register(), 5))
first_elapsed = perf_counter() - started

started = perf_counter()
second_pass = list(islice(reg.get_statistics_register(), 5))
second_elapsed = perf_counter() - started

assert first_pass == second_pass
print(f"First pass: {first_elapsed:.3f}s; cached replay: {second_elapsed:.3f}s")
display(pd.json_normalize(second_pass))

# %% [markdown]
# ## Reset the cache when you want fresh register pages
#
# Rerun the cache cell above to fetch again. Calling `configure()` also clears
# cached pages. These actions do not write anything to the server.

# %%
reg.clear_cache()
print("Register cache cleared.")
