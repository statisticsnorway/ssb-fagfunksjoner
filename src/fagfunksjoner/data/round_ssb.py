"""Reproduce the functionality of the default round function from Excel or SAS, rounding data up to a given number of decimal places.

Instead of Python's default of rounding to even.
"""

from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import TYPE_CHECKING, Any, overload

import pandas as pd


# Alias for type checking
if TYPE_CHECKING:
    pd_Series = pd.Series[Any]
else:
    pd_Series = (
        object  # Fallback to avoid runtime issues where pd.Series is not subscriptable
    )


# Overloads, output type is dependent on input type
@overload
def round_up(data: pd.DataFrame, decimal_places: int) -> pd.DataFrame: ...
@overload
def round_up(data: pd_Series, decimal_places: int) -> pd_Series: ...


# Mypy does not like getting specific with Literal[0], thats too bad
@overload
def round_up(data: int | float, decimal_places: int) -> int | float: ...
@overload
def round_up(
    data: pd._libs.missing.NAType, decimal_places: int
) -> pd._libs.missing.NAType: ...


def round_up(
    data: pd.DataFrame | pd_Series | float | pd._libs.missing.NAType,
    decimal_places: int = 0,
    col_names: str | list[str] | dict[str, int] = "",
) -> pd.DataFrame | pd_Series | int | float | pd._libs.missing.NAType:
    """Round up a number, to a given number of decimal places. Avoids Pythons default of rounding to even.

    Args:
        data: The data to round up, can be a float, Series, or DataFrame.
        decimal_places: The number of decimal places to round up to. Ignored if you send a dictionary into col_names with column names and decimal places.
        col_names: The column names to round up. If a dictionary is provided, it should map column names to the number of decimal places for each column.
            If a list is provided, it should contain the names of the columns to round up. If a string is provided, it should be the name of a single column to round up.

    Returns:
         pd.DataFrame | pd.Series | int | float: The rounded up number as an int, float, Series, or DataFrame.

    Raises:
        TypeError: If data is not a DataFrame, Series, int, float, or NAType.
    """
    if isinstance(data, pd.DataFrame):
        if isinstance(col_names, dict):
            # Assuming col_names is a dictionary with column names as keys and decimal places as values
            for col, dec in col_names.items():
                data = _apply_rounding_to_df_col(data, col, dec)
        elif isinstance(col_names, list):
            # Assuming col_names is a list of column names
            for col in col_names:
                data = _apply_rounding_to_df_col(data, col, decimal_places)
        elif isinstance(col_names, str):
            # Assuming col_names is a single column name
            data = _apply_rounding_to_df_col(data, col_names, decimal_places)
    elif isinstance(data, pd.Series):
        # If data is a Series, round it directly
        data = _set_dtype_from_decimal_places(
            data.apply(_round, decimals=decimal_places), decimal_places
        )
    elif isinstance(data, int | float | pd._libs.missing.NAType):
        data = _round(data, decimals=decimal_places)
    else:
        raise TypeError(
            "data must be a DataFrame, Series, int, float, or NAType. "
            f"Got {type(data)} instead."
        )
    return data


def _apply_rounding_to_df_col(
    df: pd.DataFrame, col_name: str, decimal_places: int
) -> pd.DataFrame:
    """Apply rounding to a specific column in a DataFrame.

    Args:
        df: The DataFrame to round.
        col_name: The name of the column to round.
        decimal_places: The number of decimal places to round to.

    Returns:
        pd.DataFrame: The DataFrame with the rounded column.
    """
    if col_name in df.columns:
        df[col_name] = _set_dtype_from_decimal_places(
            df[col_name].apply(_round, decimals=decimal_places), decimal_places
        )
    return df


def _set_dtype_from_decimal_places(
    data: pd_Series,
    decimal_places: int = 0,
) -> pd_Series:
    """Set the dtype of the data based on the number of decimal places.

    Args:
        data: The column to set the dtype for.
        decimal_places: The number of decimal places.

    Returns:
        pd_Series: The data with the updated dtype.
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
                return int(Decimal(rounded).to_integral_value())
            return float(rounded)
    return n
