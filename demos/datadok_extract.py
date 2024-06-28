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
#     display_name: ssb-fagfunksjoner
#     language: python
#     name: ssb-fagfunksjoner
# ---

from fagfunksjoner.data.datadok_extract import import_archive_data
import pandas as pd
import os

path = "/ssb/stamme01/utd/nudb/arkiv/videregaendekarakterer/g2022g2023"


def open_with_datadok(path: str) -> pd.DataFrame:
    # Correcting path for API
    dokpath = path
    if not path.startswith("$"):
        for name, stamm in os.environ.items():
            if not name.startswith("JUPYTERHUB") and path.startswith(stamm):
                dokpath = f"${name}{path.replace(stamm,'')}"
    if dokpath.endswith(".dat") or dokpath.endswith(".txt"):
        dokpath = ".".join(dokpath.split(".")[:-1])
    url_address = f'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path={dokpath}'
    
    # Correcting path in
    filepath = path
    # Flip Stamm
    for name, stamm in os.environ.items():
        if not name.startswith("JUPYTERHUB") and filepath.startswith(f"${name}"):
            end = filepath.replace(f"${name}", "")
            if end.startswith(os.sep):
                end = end[len(os.sep):]
            filepath = os.path.join(stamm, end)
            
    if filepath.endswith(".txt") or filepath.endswith(".dat"):
        ...
    else:
        if os.path.isfile(f"{filepath}.txt"):
            filepath += ".txt"
        elif os.path.isfile(f"{filepath}.dat"):
            filepath += ".dat"
        
    return import_archive_data(url_address, filepath)


vgskarak = open_with_datadok("/ssb/stamme01/utd/nudb/arkiv/videregaendekarakterer/g2022g2023.dat")

vgskarak.df

vgskarak.__dict__.keys()

vgskarak.metadata_df

open_with_datadok("/ssb/stamme01/utd/nudb/arkiv/videregaendekarakterer/g2022g2023").df.head(2)

open_with_datadok("$UTD/nudb/arkiv/videregaendekarakterer/g2022g2023.dat").df.head(2)

open_with_datadok("$UTD/nudb/arkiv/videregaendekarakterer/g2022g2023").df.head(2)


