"""Statistikkregisteret read client and shortcode utilities.

The bundled client is generated at development time; the YAML and generator
are not needed at runtime. Configure STATREG_BASE_URL or call configure() to
choose a server. The default points to the internal test environment.
"""

from . import models
from ._transport import configure
from ._utilities import (
    FuturePublishingError,
    VariantSelectionError,
    clear_cache,
    find_latest_publishing,
    find_publishings,
    find_stat_shortcode,
    get_contacts,
    get_contacts_shortcode,
    get_last_publish_date_shortcode,
    get_next_publish_date_shortcode,
    get_singles_publishings,
    get_statistic_name_shortcode,
    get_statistic_shortcode,
    get_statistics_register,
    get_variant_name,
    get_variants_shortcode,
    iter_releases_shortcode,
    raise_on_missing_future_publish,
    sections_publishings,
    select_variant_shortcode,
    single_stat,
    specific_publishing,
    time_until_publishing,
)

__all__ = [
    "FuturePublishingError",
    "VariantSelectionError",
    "clear_cache",
    "configure",
    "find_latest_publishing",
    "find_publishings",
    "find_stat_shortcode",
    "get_contacts",
    "get_contacts_shortcode",
    "get_last_publish_date_shortcode",
    "get_next_publish_date_shortcode",
    "get_singles_publishings",
    "get_statistic_name_shortcode",
    "get_statistic_shortcode",
    "get_statistics_register",
    "get_variant_name",
    "get_variants_shortcode",
    "iter_releases_shortcode",
    "models",
    "raise_on_missing_future_publish",
    "sections_publishings",
    "select_variant_shortcode",
    "single_stat",
    "specific_publishing",
    "time_until_publishing",
]
