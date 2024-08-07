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
from fagfunksjoner import open_path_datadok


# %%
tiar = "$UTD/gjfor_vgo/arkiv/tiar/g2013g2023"
arkivfil = open_path_datadok(tiar)

# %%
# Without file ending
open_path_datadok(
    "/ssb/stamme01/utd/nudb/arkiv/videregaendekarakterer/g2022g2023"
).df.head(2)

# %%
# With dollar-path
open_path_datadok("$UTD/nudb/arkiv/videregaendekarakterer/g2022g2023.dat").df.head(2)

# %%
# With dollar-path and no file ending
open_path_datadok("$UTD/nudb/arkiv/videregaendekarakterer/g2022g2023").df.head(2)
