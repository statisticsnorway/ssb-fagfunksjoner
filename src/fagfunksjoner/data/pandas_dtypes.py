"""Automatically changes dtypes on pandas dataframes using logic.
Tries to keep objects as strings if numeric, but with leading zeros.
Downcasts ints to smalles size. Changes possible columns to categoricals.
The function you most likely want is "auto_dype"."""

import pandas as pd
import gc
import json


def dtype_set_from_json(df: pd.DataFrame, json_path: str) -> pd.DataFrame:
    with open(json_path, "r") as json_file:
        json_dtypes = json.load(json_file)
    for col, dtypes in json_dtypes.items():
        if dtypes["secondary_dtype"]:
            df[col] = df[col].astype(dtypes["secondary_dtype"])
        df[col] = df[col].astype(dtypes["dtype"])
    return df


def dtype_store_json(df: pd.DataFrame, json_path: str) -> None:
    dtype_metadata = {}
    for col, dtype in df.dtypes.items():
        second_dtype = None
        if dtype == "category":
            second_dtype = str(df[col].cat.categories.dtype)
        dtype = str(dtype)
        dtype_metadata[col] = {"dtype": dtype, "secondary_dtype": second_dtype}
    with open(json_path, "w") as json_file:
        json.dump(dtype_metadata, json_file)


def auto_dtype(
    df: pd.DataFrame,
    cardinality_threshold: int = 0,
    copy_df: bool = True,
    show_memory: bool = True,
) -> pd.DataFrame:
    """Cleans up a dataframes dtypes.
    First lowers all column names.
    Tries to decodes byte strings to utf8.
    Runs pandas' convert_dtypes()
    Tries to convert object to string, and strips empty spaces
    Downcasts ints to lower versions of ints
    If cardinality_threshold is set above 0, will convert object and strings
    to categoricals, if number of unique values in the columns are below the threshold.
    """

    if show_memory:
        print("\nMemory usage before cleanup:")
        orig_size = df.memory_usage(deep=True).sum()
        print(f"{orig_size:,}")
    if copy_df:
        df = df.copy()
    # Lowercase all column names
    df.columns = [col.lower() for col in df.columns]
    # Try to decode any objects/strings into utf-8 (might be bytes)
    df = decode_bytes(df, False)
    # Have pandas do its best guess
    df = df.convert_dtypes()
    # Try to convert objects to strings
    df = object_to_strings(df, False)
    # Convert objects to floats where possible

    # Detect string/object columns that could be converted to int
    df = strings_to_int(df, False)
    # Minimize int sizes
    df = smaller_ints(df, False)

    # Convert string columns to categoricals if threshold is set
    if cardinality_threshold:
        df = categories_threshold(df, cardinality_threshold, False)
    if show_memory:
        print("\nMemory usage after cleanup:")
        new_size = df.memory_usage(deep=True).sum()
        print(f"{new_size:,}")
        print((new_size * 100) // orig_size, "% of original size.")
    return df


def decode_bytes(
    df: pd.DataFrame, copy_df: bool = True, check_row_len: int = 50
) -> pd.DataFrame:
    # Find columns containing bytes
    if len(df) < check_row_len:
        check_row_len = len(df)
    to_check = df.select_dtypes(include="object").head(check_row_len)
    byte_cols = []
    for col in to_check.columns:
        byte = False
        for row in to_check[col]:
            if isinstance(row, bytes):
                byte_cols += [col]
                break
    # Try to decode byte columns to utf-8
    fails = []
    if len(byte_cols):
        if copy_df:
            df = df.copy()
        for col in df.select_dtypes(include=["object", "string"]).columns:
            print(f"\rDecoding {col}" + " " * 40, end="")
            try:
                df[col] = df[col].str.decode("utf-8")
            except UnicodeDecodeError as e:
                print(f"\rFailed to decode {col} from bytes")
                fails += [col]
            # Shit is memory intensive, lets try collecting garbage...
            # seems to work?
            gc.collect()
    if fails:
        print("\nDecode failed on these:", fails)
    return df


def object_to_strings(df: pd.DataFrame, copy_df: bool = True) -> pd.DataFrame:
    if copy_df:
        df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        if df[col].dtype == "object":
            print(f"\rConverting {col} to string", " " * 40, end="")
            df[col] = df[col].astype("string").str.strip()
    return df


def strings_to_int(df: pd.DataFrame, copy_df: bool = True) -> pd.DataFrame:
    if copy_df:
        df = df.copy()
    for col in df.select_dtypes(include=["object", "string"]).columns:
        # check for Nones etc.
        if df[col].isna().any():
            continue
        # check for leading zeroes
        if any(df[col].str[0] == "0"):
            continue
        if df[col].str.isdigit().all():
            print(f"\rConverting {col} to int" + " " * 40, end="")
            df[col] = df[col].astype("Int64")
    return df


def smaller_ints(df: pd.DataFrame, copy_df: bool = True) -> pd.DataFrame:
    if copy_df:
        df = df.copy()
    for col in df.select_dtypes(include="Int64").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def categories_threshold(
    df: pd.DataFrame, cardinality_threshold: int = 0, copy_df: bool = True
) -> pd.DataFrame:
    if copy_df:
        df = df.copy()
    for i, num in df.select_dtypes(include=["object", "string"]).nunique().items():
        if num < cardinality_threshold:
            print("\rConverting to categorical:", i, num, " " * 40, end="")
            df[i] = df[i].astype("category")
    return df
