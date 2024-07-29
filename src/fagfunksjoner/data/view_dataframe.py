import ipywidgets as widgets
import numpy as np
import pandas as pd
from IPython.display import display


def filter_display(
    dataframe: pd.DataFrame,
    column: str,
    value: str | int | float,
    operator: str
) -> None:
    """Filter data based on args, and display the result.

    Args:
        dataframe (pd.DataFrame): The DataFrame to filter.
        column (str): Column to base filter on.
        value (str | int | float): Value to compare filter against.
        operator (str): How to compare column against value.
    """
    operator_functions = {
        ">": lambda df: df.loc[df[column] > value],
        ">=": lambda df: df.loc[df[column] >= value],
        "<=": lambda df: df.loc[df[column] <= value],
        "<": lambda df: df.loc[df[column] < value],
        "!=": lambda df: df.loc[~df[column].isin(value)],
        "==": lambda df: df.loc[df[column].isin(value)],
    }
    if operator == "==":
        if ("NaN" in str(value)) & (
            len(value) == 1
        ):  # Special treatment of missing value when only missing is selected
            display(dataframe.loc[dataframe[column].isna()])
        elif "NaN" in str(
            value
        ):  # Special treatment of missing value when missing and other is selected
            display(
                dataframe.loc[
                    (dataframe[column].isna()) | (dataframe[column].isin(value))
                ]
            )
        else:
            display(operator_functions[operator](dataframe))
    elif operator == "!=":
        if ("NaN" in str(value)) & (len(value)) == 1:
            display(dataframe.loc[~dataframe[column].isna()])
        elif "NaN" in str(value):
            display(
                dataframe.loc[
                    ~((dataframe[column].isna()) | (dataframe[column].isin(value)))
                ]
            )
        else:
            display(operator_functions[operator](dataframe))
    else:
        display(operator_functions[operator](dataframe))


def view_dataframe(
    dataframe: pd.DataFrame,
    column: str,
    operator: str = "==",
    unique_limit: int = 100
) -> widgets.HTML:
    """Display an interactive widget for filtering and viewing data in a DataFrame based on selection of values in one column

    Args:
        dataframe (pd.DataFrame): The DataFrame containing the data to be filtered.
        column (str): The column in the DataFrame to be filtered.
        operator (str, optional): The comparison operator for filtering  (may be altered during the display).
            Options: '==', '!=', '>=', '>', '<', '<='.
            Default: '=='.
        unique_limit (int, optional): The maximum number of unique values in the column
            for using '==' or '!=' operators.
            Default: 100.

    Returns:
        widgets.interactive: An interactive widget for filtering and viewing data based on the specified criteria.
            The '==' and '!=' operators use a dropdown list for multiple selection
            The other (interval) parameters us a slider

    Usage:
        ```python
        num_rows = 10
        data = {
            'hs': np.random.choice(['03010000', '30019000', '54022711'], size=num_rows),
            'value': np.random.randint(100000, 1000000, size=num_rows),
            'weight': np.random.randint(1, 10000, size=num_rows),
            'import': np.random.choice([True, False], size=num_rows),
        }

        df = pd.DataFrame(data)
        view_dataframe(dataframe=df, column='value', operator='>=')
        view_dataframe(dataframe=df, column='hs', operator='==')
        ```
    """
    operator_comparison = [">=", ">", "<", "<="]
    operator_equality = ["==", "!="]

    if operator in operator_comparison:
        if np.issubdtype(dataframe[column].dtype, np.number):
            if np.issubdtype(dataframe[column].dtype, np.integer):
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
        if np.issubdtype(dataframe[column].dtype, np.number):
            values_unique = dataframe[column].unique()
            values_unique.sort()
            values_widget = [
                val if not np.isnan(val) else "NaN" for val in values_unique
            ]
        else:
            values_unique = (
                dataframe[column].fillna("NaN").unique()
            )  # When missing values appear in non-numeric columns, we replace it with the text NaN
            values_unique.sort()
            values_widget = values_unique
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
