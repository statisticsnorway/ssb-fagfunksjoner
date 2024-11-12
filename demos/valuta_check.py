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
import fagfunksjoner
fagfunksjoner.__version__

# %%
from fagfunksjoner.api.valuta import download_exchange_rates
exchange_rates = download_exchange_rates()
exchange_rates.df

# %%
