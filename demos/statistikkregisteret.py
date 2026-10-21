# %%
"""Shortcode examples against the configured Statreg test environment."""

from itertools import islice

from fagfunksjoner.api import statistikkregisteret as reg

# %%
reg.configure(base_url="https://statistikkregisteret.intern.test.ssb.no/api")
statistic = reg.get_statistic_shortcode("hugjen")
contacts = reg.get_contacts_shortcode("hugjen")
next_publish_date = reg.get_next_publish_date_shortcode("hugjen")

# %%
variant = reg.select_variant_shortcode("kargrs", terms=["nasj", "prøv"])
variant_name = reg.get_variant_name(variant)
next_variant_date = reg.get_next_publish_date_shortcode("kargrs", variant_id=variant.id)
latest_past_date = reg.get_last_publish_date_shortcode(
    "kargrs", terms=["avslutt", "grunnskol"], include_future=False
)

# %%
# A partial iteration only fetches the pages needed; later iterations reuse them.
first_five = list(islice(reg.get_statistics_register(), 5))
reg.clear_cache()

# %%
print(first_five)
# %%
