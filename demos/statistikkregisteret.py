# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: ''
#     name: ''
# ---

# +
import os

os.chdir("../")
from fagfunksjoner.api import statistikkregisteret as reg
# -

reg.find_latest_publishing("vgu")


