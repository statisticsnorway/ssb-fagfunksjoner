# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: ssb-fagfunksjoner
#     language: python
#     name: ssb-fagfunksjoner
# ---

# %%
import os

from fagfunksjoner import ProjectRoot


# %%
print(os.getcwd())

# %%
proj_root = ProjectRoot()

# %%
print(proj_root.path)

# %%
