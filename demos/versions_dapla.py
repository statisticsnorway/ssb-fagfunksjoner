# ---
# jupyter:
#   jupytext:
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
import dapla as dp


fs = dp.FileClient().get_gcs_file_system()

# %%
base_path = "ssb-dapla-felles-data-produkt-prod"
folder_name = "versions_paths_fagfunksjoner"

# %% [markdown]
# # Make testfiles

# %%
testfile_paths = [
    "file_v1.parquet",
    "file_v1__DOC.json",
    "file_v12__DOC.json",
    "file_v2.parquet",
    "file_v11.parquet",
    "otherfile_v3.parquet",
    "otherfile_v20.parquet",
]

# %%
for file in testfile_paths:
    fs.touch("/".join([base_path, folder_name, file]))

# %% [markdown]
# # Test functions

# %%
from fagfunksjoner.paths import versions


# %%
files = fs.glob(f"{base_path}/{folder_name}/*")
files  # noqa: B018

# %%
versions.get_latest_fileversions(files)

# %%
versions.latest_version_path("/buckets/produkt/versions_paths_fagfunksjoner/file_v12__DOC")

# %%
versions.next_version_path(
    "/buckets/produkt/versions_paths_fagfunksjoner/file_v1.parquet"
)

# %%
versions.next_version_path(
    "gs://ssb-dapla-felles-data-produkt-prod/versions_paths_fagfunksjoner/otherfile_v1.parquet"
)

# %%
versions.latest_version_number(
    "ssb-dapla-felles-data-produkt-prod/versions_paths_fagfunksjoner/file_v1.parquet"
)

# %%
versions.next_version_path(
    "ssb-dapla-felles-data-produkt-prod/versions_paths_fagfunksjoner/dont_exist_v1.parquet"
)

# %%
