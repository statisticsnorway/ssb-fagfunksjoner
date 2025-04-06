from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import TypeVar

import pandas as pd


Typevar = "float_series_dataframe"
float_series_dataframe = TypeVar(
    "float_series_dataframe", bound=float | pd.Series[float | pd._libs.missing.NAType] | pd.DataFrame
)


def round_up(
    data: float_series_dataframe,
    decimal_places: int = 0,
    col_names: str | list[str] | dict[str, int] = "",
) -> float_series_dataframe:
    """Round up a number, to a given number of decimal places. Avoids Pythons default of rounding to even.

    Args:
        data: The data to round up, can be a float, Series, or DataFrame.
        decimal_places: The number of decimal places to round up to. Ignored if you send a dictionary into col_names with column names and decimal places.
        col_names: The column names to round up. If a dictionary is provided, it should map column names to the number of decimal places for each column.
            If a list is provided, it should contain the names of the columns to round up. If a string is provided, it should be the name of a single column to round up.

    Returns:
        float_series_dataframe: The rounded up number as a float, Series, or DataFrame.
    """
    if isinstance(col_names, dict) and isinstance(data, pd.DataFrame):
        # Assuming col_names is a dictionary with column names as keys and decimal places as values
        for col, dec in col_names.items():
            if col in data.columns:
                data[col] = _set_dtype_from_decimal_places(
                    data[col].apply(_round, decimals=dec), dec
                )
        return data
    elif isinstance(col_names, list) and isinstance(data, pd.DataFrame):
        # Assuming col_names is a list of column names
        for col in col_names:
            if col in data.columns:
                data[col] = _set_dtype_from_decimal_places(
                    data[col].apply(_round, decimals=decimal_places), decimal_places
                )
        return data
    elif isinstance(col_names, str) and isinstance(data, pd.DataFrame):
        # Assuming col_names is a single column name
        if col_names in data.columns:
            data[col_names] = _set_dtype_from_decimal_places(
                data[col_names].apply(_round, decimals=decimal_places), decimal_places
            )
        return data
    elif isinstance(data, pd.Series):
        # If data is a Series, round it directly
        data = _set_dtype_from_decimal_places(
            data.apply(_round, decimals=decimal_places), decimal_places
        )
        return data
    return _round(data, decimals=decimal_places)


def _set_dtype_from_decimal_places(
    data: pd.Series,
    decimal_places: int = 0,
) -> pd.Series:
    """Set the dtype of the data based on the number of decimal places.

    Args:
        data: The data to set the dtype for.
        decimal_places: The number of decimal places.

    Returns:
        float | pd.Series | pd.DataFrame: The data with the updated dtype.
    """
    if decimal_places == 0:
        return data.astype("Int64")
    else:
        return data.astype("Float64")


def _round(
    n: float | pd._libs.missing.NAType,
    decimals: int = 0,
) -> float | int | pd._libs.missing.NAType:
    if pd.isna(n):
        return pd.NA
    elif n or n == 0:
        with localcontext() as ctx:
            ctx.rounding = ROUND_HALF_UP
            rounded = round(Decimal(n), decimals)
            if decimals == 0:
                return Decimal(rounded).to_integral_value()
            return float(rounded)
    return n
