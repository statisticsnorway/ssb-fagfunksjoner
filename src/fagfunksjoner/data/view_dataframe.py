from collections.abc import Callable

import ipywidgets as widgets
import pandas as pd
from IPython.display import display


def filter_display(
    dataframe: pd.DataFrame,
    column: str,
    value: widgets.Widget,
    operator: widgets.Dropdown,
) -> None:
    """Filter data based on args, and display the result.

    Args:
        dataframe (pd.DataFrame): The DataFrame to filter.
        column (str): Column to base filter on.
        value (widgets.Widget): Widget containing the value to compare filter against.
        operator (widgets.Dropdown): Widget containing how to compare column against value.
    """
    # Extract the actual value from the widgets
    filter_value = value.value
    filter_operator = operator.value

    operator_functions: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
        ">": lambda df: df.loc[df[column] > filter_value],
        ">=": lambda df: df.loc[df[column] >= filter_value],
        "<=": lambda df: df.loc[df[column] <= filter_value],
        "<": lambda df: df.loc[df[column] < filter_value],
        "!=": lambda df: df.loc[~df[column].isin(filter_value)],
        "==": lambda df: df.loc[df[column].isin(filter_value)],
    }

    if filter_operator == "==" and "NaN" in filter_value:
        display(dataframe.loc[dataframe[column].isna() | dataframe[column].isin([val for val in filter_value if val != "NaN"])])  # type: ignore[no-untyped-call]
    elif filter_operator == "!=" and "NaN" in filter_value:
        display(dataframe.loc[~dataframe[column].isna() & ~dataframe[column].isin([val for val in filter_value if val != "NaN"])])  # type: ignore[no-untyped-call]
    else:
        display(operator_functions[filter_operator](dataframe))  # type: ignore[no-untyped-call]


def view_dataframe(
    dataframe: pd.DataFrame, column: str, operator: str = "==", unique_limit: int = 100
) -> widgets.HTML | widgets.interactive:
    """Display an interactive widget for filtering and viewing data in a DataFrame based on selection of values in one column.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing the data to be filtered.
        column (str): The column in the DataFrame to be filtered.
        operator (str): The comparison operator for filtering (may be altered during the display).
            Options: '==', '!=', '>=', '>', '<', '<='.
            Default: '=='.
        unique_limit (int): The maximum number of unique values in the column
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
