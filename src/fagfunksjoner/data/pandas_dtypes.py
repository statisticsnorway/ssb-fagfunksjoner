"""Automatically changes dtypes on pandas dataframes using logic.

Tries to keep objects as strings if numeric, but with leading zeros.
Downcasts ints to smalles size. Changes possible columns to categoricals.
The function you most likely want is "auto_dype".
"""

import gc
import json

import pandas as pd

from fagfunksjoner.fagfunksjoner_logger import logger


def dtype_set_from_json(df: pd.DataFrame, json_path: str) -> pd.DataFrame:
    """Use a stored json to change the dtypes of a dataframe to match what was stored.

    Args:
        df: The Dataframe to manipulate towards the stored dtypes.
        json_path: The jsonfile containing the dtypes on the columns.

    Returns:
        pd.DataFrame: The manipulated dataframe, with newly set dtypes.
    """
    with open(json_path) as json_file:
        json_dtypes = json.load(json_file)
    for col, dtypes in json_dtypes.items():
        if dtypes["secondary_dtype"]:
            df[col] = df[col].astype(dtypes["secondary_dtype"])
        df[col] = df[col].astype(dtypes["dtype"])
    return df


def dtype_store_json(df: pd.DataFrame, json_path: str) -> None:
    """Store the dtypes of a dataframes columns as a json for later reference.

    Args:
        df: The dataframe to look at for column names and dtypes.
        json_path: The path to the jsonfile, to store dtypes in.
    """
    dtype_metadata = {}
    for col, dtype in df.dtypes.items():
        second_dtype = None
        if dtype == "category":
            second_dtype = str(df[col].cat.categories.dtype)
        dtype = str(dtype)
        dtype_metadata[col] = {"dtype": dtype, "secondary_dtype": second_dtype}
    with open(json_path, "w") as json_file:
        json_file.write(
            json.dumps(dtype_metadata)
        )  # Needs to match test case in how it writes?


def auto_dtype(
    df: pd.DataFrame,
    cardinality_threshold: int = 0,
    copy_df: bool = True,
    show_memory: bool = True,
) -> pd.DataFrame:
    """Clean up a dataframes dtypes.

    First lowers all column names.
    Tries to decodes byte strings to utf8.
    Runs pandas' convert_dtypes()
    Tries to convert object to string, and strips empty spaces
    Downcasts ints to lower versions of ints
    If cardinality_threshold is set above 0, will convert object and strings
    to categoricals,
    if number of unique values in the columns are below the threshold.

    Args:
        df: The dataframe to manipulate
        cardinality_threshold: Less unique values in columns than this threshold,
            means it should be converted to a categorical. Defaults to 0, meaning no conversion to categoricals.
        copy_df: The reverse of inplace, make a copy in memory. This may give a memory impact, but be safer. Defaults to True.
        show_memory: Show the user how much memory was saved by doing the conversion, does require some processing. Defaults to True.

    Returns:
        pd.DataFrame: _description_
    """
    if show_memory:
        logger.info("\nMemory usage before cleanup:")
        orig_size = df.memory_usage(deep=True).sum()
        logger.info(f"{orig_size:,}")
    if copy_df:
        df = df.copy()
    # Lowercase all column names
    df = df.rename(columns={col: col.lower() for col in df.columns})
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
        logger.info("Memory usage after cleanup:")
        new_size = df.memory_usage(deep=True).sum()
        logger.info(f"{new_size:,}")
        logger.info(f"{(new_size * 100) // orig_size}% of original size.")
    return df


def decode_bytes(
    df: pd.DataFrame, copy_df: bool = True, check_row_len: int = 50
) -> pd.DataFrame:
    """Check object columns if they contain bytes and should be attempted to convert to real utf8 strings.

    Args:
        df: The dataframe to check.
        copy_df: The reverse of inplace, make a copy in memory. This may give a memory impact, but be safer. Defaults to True.
        check_row_len: How many rows to look for byte-content in, conserves processing, but might miss columns if set too low. Defaults to 50.

    Returns:
        pd.DataFrame: The dataframe with converted byte-columns to string-columns.
    """
    # Find columns containing bytes
    if len(df) < check_row_len:
        check_row_len = len(df)
    to_check = df.select_dtypes(include="object").head(check_row_len)
    byte_cols = []
    for col in to_check.columns:
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
            logger.info(f"Decoding {col}")
            try:
                df[col] = df[col].str.decode("utf-8").astype("string[pyarrow]")
            except UnicodeDecodeError:
                logger.info(f"\rFailed to decode {col} from bytes")
                fails += [col]
            # Shit is memory intensive, lets try collecting garbage...
            # seems to work?
            gc.collect()
    if fails:
        logger.warning("Decode failed on these:", fails)
    return df


def object_to_strings(df: pd.DataFrame, copy_df: bool = True) -> pd.DataFrame:
    """Convert columns that are still "object", to pyarrow strings.

    Args:
        df: The dataframe to manipulate.
        copy_df: The reverse of inplace, make a copy in memory. This may give a memory impact, but be safer. Defaults to True.

    Returns:
        pd.DataFrame: The modified dataframe.
    """
    if copy_df:
        df = df.copy()
    for col in df.select_dtypes(include=["object", "string"]).columns:
        logger.info(f"Converting {col} to string")
        df[col] = df[col].astype("string[pyarrow]").str.strip()
    return df


def strings_to_int(df: pd.DataFrame, copy_df: bool = True) -> pd.DataFrame:
    """Checks string columns to see if their content can be converted safely to ints.

    This conserves A LOT of storage and memory.

    Args:
        df: The dataframe to manipulate.
        copy_df: The reverse of inplace, make a copy in memory. This may give a memory impact, but be safer. Defaults to True.

    Returns:
        pd.DataFrame: The manipulated dataframe.
    """
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
            logger.info(f"Converting {col} to int")
            df[col] = df[col].astype("Int64")
    return df


def smaller_ints(df: pd.DataFrame, copy_df: bool = True) -> pd.DataFrame:
    """Downcasts ints to smaller int-dtypes to conserve space.

    Args:
        df: The dataframe to manipulate.
        copy_df: The reverse of inplace, make a copy in memory. This may give a memory impact, but be safer. Defaults to True.

    Returns:
        pd.DataFrame: The manipulated dataframe.
    """
    if copy_df:
        df = df.copy()
    for col in df.select_dtypes(include="Int64").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def categories_threshold(
    df: pd.DataFrame, cardinality_threshold: int = 0, copy_df: bool = True
) -> pd.DataFrame:
    """Convert to categoricals using a threshold of unique values.

    Args:
        df: The dataframe to convert to categoricals on.
        cardinality_threshold: Less unique values in columns than this threshold,
            means it should be converted to a categorical. Defaults to 0, meaning no conversion to categoricals.
        copy_df: The reverse of inplace, make a copy in memory. This may give a memory impact, but be safer. Defaults to True.

    Returns:
        pd.DataFrame: The dataframe with converted columns to categoricals.
    """
    if copy_df:
        df = df.copy()
    str_cols = df.select_dtypes(include=["object", "string"])
    for i, num in str_cols.nunique().items():
        if num <= cardinality_threshold:
            logger.info(f"Converting to categorical: {i} {num}")
            df[i] = df[i].astype("category")
    return df
