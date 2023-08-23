"""The background for these functions is a common operation before publishing to the "statbank" at statistics Norway.
All combinations (including total-groups), over all categorical codes, in a set of columns, need to have their numbers aggregated.
This has some similar functionality to "proc means" in SAS.
"""


import pandas as pd
from itertools import combinations


def all_combos(df: pd.DataFrame,
               columns: list,
               *agg_args, **agg_kwargs) -> pd.DataFrame:
    df2 = df.copy()
    # Hack, for å beholde tomme grupper + observed i groupbyen
    for col in columns:
        df2[col] = df2[col].astype("category")
    # Lager alle kombinasjoner av grupperingskolonnene
    tab = pd.DataFrame()
    for x in range(5):
        groups = combinations(columns, x)
        for group in groups:
            print(x, list(group))
            if group:
                df2 = (
                    df2.groupby(list(group), dropna=False, observed=False)
                    .agg(*agg_args, **agg_kwargs)
                    .reset_index()
                )
            else:
                df2 = pd.DataFrame(df2
                    .agg(*agg_args, **agg_kwargs)
                    .T).T
            for col in columns:
                if col not in df2.columns:
                    df2[col] = pd.NA
            tab = pd.concat([tab, df2])
    for col in columns:
        tab[col] = tab[col].astype("string")
    return tab

def fill_na_dict(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    for col, fill_val in vmapping.items():
        df[col] = df[col].fillna(fill_val)
    return df
