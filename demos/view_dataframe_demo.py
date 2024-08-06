# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: ssb-fagfunksjoner
#     language: python
#     name: ssb-fagfunksjoner
# ---

# %% [markdown]
# # view_dataframe
# This is a demo of the function view_dataframe. view_dataframe views a dataframe in a dynamically way using ipywidgets

# %%
import ipywidgets
import numpy as np
import pandas as pd
from IPython.display import display


# %%
from fagfunksjoner import view_dataframe


# %%
ipywidgets.interactive()


# %% [markdown]
# ## Generate test data
# Here is a function that generates some test data. We can choose the share of missing values we shall have on the column 'value'


# %%
def generate_test_dataframe(
    num_rows: int = 10, missing_percentage: float = 0.1
) -> pd.DataFrame:
    """Generate testdataset to view with widget.

    Args:
        num_rows (int): Number of rows you want in your testdataset. Defaults to 10.
        missing_percentage (float): How many missingvalues you want as a percentage. Defaults to 0.1.

    Returns:
        pd.DataFrame: The testdataset.
    """
    np.random.seed(42324)  # For reproducibility

    data = {
        "hs": np.random.choice(["03010000", "30019000", "54022711"], size=num_rows),
        "value": np.random.choice(list(range(10000, 1000000)), size=num_rows).astype(
            float
        ),
        "weight": np.random.randint(1, 10000, size=num_rows),
        "import": np.random.choice([True, False], size=num_rows),
    }

    # Introduce missing values in 'value' column
    num_missing = int(missing_percentage * num_rows)
    indices_to_make_missing = np.random.choice(
        num_rows, size=num_missing, replace=False
    )
    data["value"][indices_to_make_missing] = None
    df = pd.DataFrame(data)

    # Add a new column 'price'
    df["price"] = df["value"] / df["weight"]

    return df


# %% [markdown]
# ## Generate test data and use the function with comaprison operators
# First we use the slider to select data. It is used when we choose comparison operators (>,>=, <, <=). The default value for the slider bar is the median of the values.

# %%
foreign_trade = generate_test_dataframe(50, missing_percentage=0.1)
view_dataframe(dataframe=foreign_trade, column="value", operator=">=")

# %% [markdown]
# ## Use the function with equality parameters
# When we use equality operators (==, !=), a multiple dropdown list is used. Now we can select data from this multiple select dropdown menu. If wanted, mark more than one line in the dropdown list by using the *shift* or *ctrl* key when you select values.

# %%
view_dataframe(dataframe=foreign_trade, column="hs", operator="==")

# %% [markdown]
# ## Sort values that is used by the view_dataframe function

# %%
view_dataframe(
    dataframe=foreign_trade.sort_values("price"), column="price", operator=">="
)

# %% [markdown]
# ## string column with missing values
# There are always issues when missing values appears. One special situation is when there are missing values in a string column. It is taken care of.

# %%
foreign_trade["value_o"] = foreign_trade["value"].astype(str)
display(foreign_trade.dtypes)
view_dataframe(dataframe=foreign_trade, column="value_o", operator="==")

# %% [markdown]
# ## Boolean columns are not treated as numeric columns

# %%
view_dataframe(dataframe=foreign_trade, column="import", operator=">=")

# %%
view_dataframe(dataframe=foreign_trade, column="import", operator="==")

# %% [markdown]
# ## The unique_limit parameter
# If we have more than 100 different values to use with the equality operators, we get an error message. We can change the limit, however we should not use the equality operators on columns with very many unique values

# %%
view_dataframe(
    dataframe=foreign_trade, column="value_o", operator="==", unique_limit=1000
)
