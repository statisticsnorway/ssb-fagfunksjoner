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
import os

os.chdir("../")
from fagfunksjoner.api import statistikkregisteret as reg


# %%
reg.find_latest_publishing("vgu")
