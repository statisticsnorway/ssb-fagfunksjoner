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
from fagfunksjoner.prodsone import saspy_ssb

saspy_ssb.swap_server(2)

# %%
saspy_ssb.saspy_session()
