# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Import file description from Datadok and use that to import archive file
# We use the path in Datadok and the name of the archive file as arguments to the function

# %%
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
import xml.etree.ElementTree as ET
from fagfunksjoner.data.datadok_extract import *

# %% [markdown]
# ## Create test data

# %%
import random

def generate_random_text(length, lines):
    result = []
    for _ in range(lines):
        line = ''.join(str(random.randint(0, 9)) for _ in range(length))
        result.append(line)
    return result

def write_to_file(filename, lines):
    with open(filename, 'w') as file:
        for line in lines:
            file.write(line + '\n')

# Konfigurasjoner
length_of_line = 1389
number_of_lines = 10
filename = 'arkivfil.txt'

# Generer tekst
random_text_lines = generate_random_text(length_of_line, number_of_lines)

# Skriv til fil
write_to_file(filename, random_text_lines)

print(f"Dataene er skrevet til {filename}")


# %% [markdown]
# ## Hente fra api til Datadok
# Vi har et api til datadok og det returnerer filbeskrivelse som en html-fil. Det kan f.eks. kalles slik
#
# `curl -i 'ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path=$ENERGI/er_eb/arkiv/grunnlag/g1990'
# `
#
# Den interne metadataportalen http://www.byranettet.ssb.no/metadata/ har også alle filbeskrivelsene og filvariablene. 

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path=$ENERGI/er_eb/arkiv/grunnlag/g1990'
archivefile = 'arkivfil.txt'
archive_data = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
display(archive_data.df)
display(archive_data.codelist_df)
archive_data.metadata_df

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path=$FATS/arkiv/sb_pop/2019'
archivefile = 'arkivfil.txt'
archive_data = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
display(archive_data.df)
display(archive_data.codelist_df)
archive_data.metadata_df

# %%
display(archive_data.df)
display(archive_data.metadata_df)
display(archive_data.codelist_df)
display(archive_data.codelist_dict)
display(archive_data.names)
display(archive_data.widths)
display(archive_data.datatypes)

# %% [markdown]
# ## USe id instead of path
# The problem with using the id is that it is diffucult to find

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionById?id=urn:ssb:dataset:datadok:1288400'
archivefile = 'arkivfil.txt'
archive_df = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
archive_df.df

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionById?id=urn:ssb:dataset:datadok:5031711'
archivefile = 'arkivfil.txt'
archive_df = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
archive_df.df

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionById?id=urn:ssb:dataset:datadok:4582427'
archivefile = 'arkivfil.txt'
archive_df = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
display(archive_df.df)
archive_df.metadata_df

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionById?id=urn:ssb:dataset:datadok:962'
archivefile = 'arkivfil.txt'
archive_df = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
display(archive_df.df)
archive_df.metadata_df

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionById?id=urn:ssb:dataset:datadok:1288400'
archivefile = 'arkivfil.txt'
archive_data = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
display(archive_data.metadata_df)
display(archive_data.codelist_df)
archive_data.df

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionById?id=urn:ssb:dataset:datadok:5031711'
archivefile = 'arkivfil.txt'
archive_data = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
display(archive_data.metadata_df)
display(archive_data.codelist_df)
archive_data.df

# %%
archive_desc_xml = 'http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionById?id=urn:ssb:dataset:datadok:4582427'
archivefile = 'arkivfil.txt'
archive_data = import_archive_data(
    archive_desc_xml=archive_desc_xml,
    archive_file=archivefile
)
display(archive_data.df)
display(archive_data.codelist_df)
archive_data.metadata_df
