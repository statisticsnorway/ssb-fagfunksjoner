# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: ssb-fagfunksjoner
#     language: python
#     name: ssb-fagfunksjoner
# ---

# %%
from fagfunksjoner.api import statistikkregisteret as reg


# %%
shortcode = "vgu"

# %%
single = reg.single_stat()

# %%
print(single.navn.navn[0].text)
print(single.eierseksjon.statid)
for kontaktinfo in single.kontakter:
    for kontakt in kontaktinfo.navn:
        if kontakt.lang == "no":
            print(kontakt.text)

# %%
reg.find_stat_shortcode(shortcode)

# %%
reg.find_latest_publishing(shortcode)

# %%
reg.find_publishings()

# %%
reg.specific_publishing()

# %%
reg.time_until_publishing("vgu")

# %%
import datetime

from fagfunksjoner.api import statistikkregisteret as reg


kortkode = "vgu"

if not datetime.timedelta(0) < reg.time_until_publishing(kortkode):
    raise ValueError("HAR DU IKKE MELDT PUBLISERING!?!?!?")

# %%
