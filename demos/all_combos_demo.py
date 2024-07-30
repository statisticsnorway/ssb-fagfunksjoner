# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: ''
#     name: ''
# ---

# +
import os

os.chdir("../")
# +
import pandas as pd

from fagfunksjoner import all_combos_agg

data = {
    "alder": [20, 60, 33, 33, 20],
    "kommune": ["0301", "3001", "0301", "5401", "0301"],
    "kjonn": ["1", "2", "1", "2", "2"],
    "inntekt": [1000000, 120000, 220000, 550000, 50000],
    "formue": [25000, 50000, 33000, 44000, 90000],
}
pers = pd.DataFrame(data)

agg1 = all_combos_agg(
    pers, groupcols=["kjonn"], keep_empty=True, aggargs={"inntekt": ["mean", "sum"]}
)
display(agg1)

agg2 = all_combos_agg(
    pers, groupcols=["kjonn", "alder"], aggargs={"inntekt": ["mean", "sum"]}
)
display(agg2)
# -

agg3 = all_combos_agg(
    pers,
    groupcols=["kjonn", "alder"],
    grand_total="Grand total",
    aggargs={"inntekt": ["mean", "sum"], "formue": ["sum"]},
)
display(agg3)

fillna_dict = {"kjonn": "Total kjønn", "alder": "Total alder"}
agg4 = all_combos_agg(
    pers,
    groupcols=["kjonn", "alder"],
    fillna_dict=fillna_dict,
    aggargs={"inntekt": ["mean", "sum"], "formue": ["count", "min", "max"]},
    grand_total=fillna_dict,
)
display(agg4)

pers["antall"] = 1
groupcols = pers.columns[0:3].tolist()
func_dict = {"inntekt": ["mean", "sum"], "formue": ["sum", "std", "count"]}
fillna_dict = {
    "kjonn": "Total kjønn",
    "alder": "Total alder",
    "kommune": "Total kommune",
}
agg5 = all_combos_agg(
    pers,
    groupcols=groupcols,
    aggargs=func_dict,
    fillna_dict=fillna_dict,
    grand_total="All",
)
display(agg5)


# +
def dataset1() -> pd.DataFrame:
    """Return a testdataset.

    Returns:
        pd.DataFrame: The test dataset.
    """
    return pd.DataFrame(
        {
            "sex": ["Mann", "Kvinne", "Mann", "Mann", "Kvinne", "Mann"],
            "age": ["40", "50", "60", "50", "60", "40"],
            "points": [100, 90, 80, 70, 60, 50],
        }
    )


def test_grand_total() -> None:
    """Tests the testdataset with the all_combos_agg function."""
    data_in = dataset1()
    result = all_combos_agg(
        data_in,
        ["sex", "age"],
        {"points": sum},
        fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
        keep_empty=True,
        grand_total="I alt",
    )
    assert "I alt" in result["sex"].to_list() and "I alt" in result["age"].to_list()
    assert (
        "Begge kjoenn" in result["sex"].to_list()
        and "Alle aldre" in result["age"].to_list()
    )


df = test_grand_total()
# -
