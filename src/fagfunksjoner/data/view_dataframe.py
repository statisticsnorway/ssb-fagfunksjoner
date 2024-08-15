from collections.abc import Callable

import ipywidgets as widgets
import pandas as pd
from IPython.display import display


def filter_display(
    dataframe: pd.DataFrame,
    column: str,
    value: str | int | float | tuple[str | int | float, ...],
    operator: str,
) -> None:
    """Filter data based on args, and display the result.

    Args:
        dataframe: The DataFrame to filter.
        column: Column to base filter on.
        value:The value to compare filter against.
        operator: How to compare column against value.

    Returns:
        None: only has visual side-effects

    Raises:
        TypeError: On combinations of value and operator we can't handle.
    """
    if operator not in ["!=", "=="]:
        if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
            value_simple: str | int | float = value
        else:
            raise TypeError("Cant handle this type of value with this operator.")
        operator_functions_simple: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
            ">": lambda df: df.loc[df[column] > value_simple],
            ">=": lambda df: df.loc[df[column] >= value_simple],
            "<=": lambda df: df.loc[df[column] <= value_simple],
            "<": lambda df: df.loc[df[column] < value_simple],
        }
        display(operator_functions_simple[operator](dataframe))  # type: ignore[no-untyped-call]
        return None
    elif operator in ["!=", "=="]:
        if isinstance(value, tuple):
            value_list: tuple[str | int | float, ...] = value
        else:
            raise TypeError(
                f"Cant handle this type of value {value} {type(value)} with this operator {operator}."
            )
        operator_functions_list: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
            "!=": lambda df: df.loc[~df[column].isin(value_list)],
            "==": lambda df: df.loc[df[column].isin(value_list)],
        }
        if operator == "==" and "NaN" in value_list:
            display(dataframe.loc[dataframe[column].isna() | dataframe[column].isin([val for val in value_list if val != "NaN"])])  # type: ignore[no-untyped-call]
        elif operator == "!=" and "NaN" in value_list:
            display(dataframe.loc[~dataframe[column].isna() & ~dataframe[column].isin([val for val in value_list if val != "NaN"])])  # type: ignore[no-untyped-call]
        else:
            display(operator_functions_list[operator](dataframe))  # type: ignore[no-untyped-call]
        return None


def view_dataframe(
    dataframe: pd.DataFrame, column: str, operator: str = "==", unique_limit: int = 100
) -> widgets.HTML | widgets.interactive:
    """Display an interactive widget for filtering and viewing data in a DataFrame based on selection of values in one column.

    Args:
        dataframe: The DataFrame containing the data to be filtered.
        column: The column in the DataFrame to be filtered.
        operator: The comparison operator for filtering (may be altered during the display).
            Options: '==', '!=', '>=', '>', '<', '<='.
            Default: '=='.
        unique_limit: The maximum number of unique values in the column
            for using '==' or '!=' operators.
            Default: 100.

    Returns:
        widgets.interactive: An interactive widget for filtering and viewing data based on the specified criteria.
            The '==' and '!=' operators use a dropdown list for multiple selection
            The other (interval) parameters us a slider
    """
    operator_comparison = [">=", ">", "<", "<="]
    operator_equality = ["==", "!="]

    if operator in operator_comparison:
        if pd.api.types.is_numeric_dtype(dataframe[column]):
            if pd.api.types.is_integer_dtype(dataframe[column]):
                slider_widget = widgets.IntSlider
            else:
                slider_widget = widgets.FloatSlider

            filter_type_select = widgets.Dropdown(
                options=operator_comparison, value=operator, description="Operator"
            )
            threshold_slider = slider_widget(
                min=dataframe[column].min(),
                max=dataframe[column].max(),
                step=1,
                value=dataframe[column].median(),
                description="Value",
            )
            return widgets.interactive(
                filter_display,
                dataframe=widgets.fixed(dataframe),
                column=widgets.fixed(column),
                operator=filter_type_select,
                value=threshold_slider,
            )
        else:
            return widgets.HTML(
                value="<b>Error:</b> Comparison operators are only allowed for numeric columns. Choose == or != instead."
            )
    elif operator in ["==", "!="]:
        num_unique = dataframe[column].nunique()
        if num_unique > unique_limit:
            return widgets.HTML(
                value=f"<b>Error:</b> The number of unique values in column '{column}' is {num_unique}, which exceeds the limit of {unique_limit}. You can change the maximum number of unique values with the unique_limit parameter or switch to a comparison operator."
            )

        filter_type_select = widgets.Dropdown(
            options=operator_equality, value=operator, description="Operator"
        )
        if pd.api.types.is_numeric_dtype(dataframe[column]):
            values_unique = dataframe[column].unique()
            values_unique.sort()
            values_widget = [
                val if not pd.isna(val) else "NaN" for val in values_unique
            ]
        else:
            values_unique = (
                dataframe[column].fillna("NaN").unique()
            )  # When missing values appear in non-numeric columns, we replace it with the text NaN
            values_unique.sort()
            values_widget = list(values_unique)
        values_select = widgets.SelectMultiple(
            options=values_widget, description="Value(s)", value=[values_widget[0]]
        )
        return widgets.interactive(
            filter_display,
            dataframe=widgets.fixed(dataframe),
            column=widgets.fixed(column),
            operator=filter_type_select,
            value=values_select,
        )
    else:
        return widgets.HTML(
            value="<b>Error:</b> Invalid operator. Use one of the following: '==', '!=', '>=', '>', '<', '<='"
        )
