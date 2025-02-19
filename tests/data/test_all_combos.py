import math

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from fagfunksjoner.data.pandas_combinations import (
    all_combos_agg,
    all_combos_agg_inclusive,
)


def convert_to_category(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert specified columns of a DataFrame to categorical type."""
    for col in columns:
        df[col] = df[col].astype("category")
    return df


def dataset1():
    df = pd.DataFrame(
        {
            "sex": ["Mann", "Kvinne", "Mann", "Mann", "Kvinne", "Mann"],
            "age": ["40", "50", "60", "50", "60", "40"],
            "points": [100, 90, 80, 70, 60, 50],
        }
    )
    return convert_to_category(df, ["sex", "age"])


def doc_data():
    df = pd.DataFrame(
        {
            "alder": [20, 60, 33, 33, 20],
            "kommune": ["0301", "3001", "0301", "5401", "0301"],
            "kjonn": ["1", "2", "1", "2", "2"],
            "inntekt": [1000000, 120000, 220000, 550000, 50000],
            "formue": [25000, 50000, 33000, 44000, 90000],
        }
    )
    return convert_to_category(df, ["kommune", "kjonn"])


def data_out():
    df = pd.DataFrame(
        {
            "sex": {
                0: "Begge kjoenn",
                1: "Begge kjoenn",
                2: "Begge kjoenn",
                3: "Kvinne",
                4: "Mann",
                5: "Kvinne",
                6: "Kvinne",
                7: "Mann",
                8: "Mann",
                9: "Mann",
            },
            "age": {
                0: "40",
                1: "50",
                2: "60",
                3: "Alle aldre",
                4: "Alle aldre",
                5: "50",
                6: "60",
                7: "40",
                8: "50",
                9: "60",
            },
            "points": {
                0: 150,
                1: 160,
                2: 140,
                3: 150,
                4: 300,
                5: 90,
                6: 60,
                7: 150,
                8: 70,
                9: 80,
            },
            "level": {0: 1, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 3, 8: 3, 9: 3},
            "ways": {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2, 6: 2, 7: 2, 8: 2, 9: 2},
        }
    )
    return convert_to_category(df, ["sex", "age"])


def data_out_keep_empty():
    df = pd.DataFrame(
        {
            "sex": {
                0: "Begge kjoenn",
                1: "Begge kjoenn",
                2: "Begge kjoenn",
                3: "Kvinne",
                4: "Mann",
                5: "Kvinne",
                6: "Kvinne",
                7: "Kvinne",
                8: "Mann",
                9: "Mann",
                10: "Mann",
            },
            "age": {
                0: "40",
                1: "50",
                2: "60",
                3: "Alle aldre",
                4: "Alle aldre",
                5: "40",
                6: "50",
                7: "60",
                8: "40",
                9: "50",
                10: "60",
            },
            "points": {
                0: 150,
                1: 160,
                2: 140,
                3: 150,
                4: 300,
                5: 0,
                6: 90,
                7: 60,
                8: 150,
                9: 70,
                10: 80,
            },
            "level": {
                0: 1,
                1: 1,
                2: 1,
                3: 2,
                4: 2,
                5: 3,
                6: 3,
                7: 3,
                8: 3,
                9: 3,
                10: 3,
            },
            "ways": {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2, 6: 2, 7: 2, 8: 2, 9: 2, 10: 2},
        }
    )
    return convert_to_category(df, ["sex", "age"])


def test_simple_sum_fillna():
    data_in = dataset1()
    result = (
        all_combos_agg(
            df=data_in,
            groupcols=["sex", "age"],
            aggargs={"points": sum},
            fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
        )
        .sort_values(by=["level", "sex", "age"])
        .reset_index(drop=True)
    )
    expected = data_out().sort_values(by=["level", "sex", "age"]).reset_index(drop=True)

    assert_frame_equal(left=result, right=expected, check_categorical=False)


def test_fillna_dict_keep_empty():
    data_in = dataset1()
    result = (
        all_combos_agg(
            df=data_in,
            groupcols=["sex", "age"],
            aggargs={"points": sum},
            fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
            keep_empty=True,
        )
        .sort_values(by=["level", "sex", "age"])
        .reset_index(drop=True)
    )
    expected = (
        data_out_keep_empty()
        .sort_values(by=["level", "sex", "age"])
        .reset_index(drop=True)
    )
    assert_frame_equal(left=result, right=expected, check_categorical=False)


def test_agg_simplified():
    data_in = dataset1()
    result = (
        all_combos_agg(
            df=data_in,
            groupcols=["sex", "age"],
            valuecols=["points"],
            fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
            keep_empty=True,
        )
        .sort_values(by=["level", "sex", "age"])
        .reset_index(drop=True)
    )
    expected = (
        data_out_keep_empty()
        .sort_values(by=["level", "sex", "age"])
        .reset_index(drop=True)
    )
    assert_frame_equal(left=result, right=expected, check_categorical=False)


def test_agg_nospecs():
    data_in = dataset1()
    result = (
        all_combos_agg(
            df=data_in,
            groupcols=["sex", "age"],
            fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
            keep_empty=True,
        )
        .sort_values(by=["level", "sex", "age"])
        .reset_index(drop=True)
    )
    expected = (
        data_out_keep_empty()
        .sort_values(by=["level", "sex", "age"])
        .reset_index(drop=True)
    )
    assert_frame_equal(left=result, right=expected, check_categorical=False)


def test_grand_total():
    data_in = dataset1()
    result = all_combos_agg(
        df=data_in,
        groupcols=["sex", "age"],
        aggargs={"points": sum},
        fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
        keep_empty=True,
        grand_total="I alt",
    )
    assert "I alt" in result["sex"].to_list() and "I alt" in result["age"].to_list()
    assert (
        "Begge kjoenn" in result["sex"].to_list()
        and "Alle aldre" in result["age"].to_list()
    )


def test_fromdoc_1():
    result = all_combos_agg(
        df=doc_data(),
        groupcols=["kjonn"],
        keep_empty=True,
        aggargs={"inntekt": ["mean", "sum"]},
    )
    assert len(result) == 2
    assert len(result.columns) == 5


def test_fromdoc_2():
    result = all_combos_agg(
        df=doc_data(),
        groupcols=["kjonn", "alder"],
        aggargs={"inntekt": ["mean", "sum"]},
    )
    assert len(result) == 10
    assert len(result.columns) == 6


def test_fromdoc_3():
    result = all_combos_agg(
        df=doc_data(),
        groupcols=["kjonn", "alder"],
        grand_total="Grand total",
        aggargs={"inntekt": ["mean", "sum"], "formue": ["sum"]},
    )
    assert len(result) == 11
    assert len(result.columns) == 7


def test_fromdoc_4():
    result = all_combos_agg(
        df=doc_data(),
        groupcols=["kjonn", "alder"],
        fillna_dict={"kjonn": "Total kjønn", "alder": "Total alder"},
        aggargs={"inntekt": ["mean", "sum"], "formue": ["count", "min", "max"]},
        grand_total="Total",
    )
    assert len(result) == 11
    assert len(result.columns) == 9


def test_fromdoc_5():
    data_in = doc_data()
    groupcols = data_in.columns[0:3].tolist()
    func_dict = {"inntekt": ["mean", "sum"], "formue": ["sum", "std", "count"]}
    fillna_dict = {
        "kjonn": "Total kjønn",
        "alder": "Total alder",
        "kommune": "Total kommune",
    }
    result = all_combos_agg(
        df=data_in,
        groupcols=groupcols,
        aggargs=func_dict,
        fillna_dict=fillna_dict,
        grand_total="All",
    )
    assert len(result) == 27
    assert len(result.columns) == 10


def test_combos_inclusive():
    # Define the categorical bins based on the metadata
    gender_bins = {"1": "Menn", "2": "Kvinner"}

    # Generate synthetic data
    np.random.seed(42)
    num_samples = 100

    synthetic_data = pd.DataFrame(
        {
            "Tid": np.random.choice(["2021", "2022", "2023"], num_samples),
            "UtdanningOppl": np.random.choice(list(range(1, 19)), num_samples),
            "Kjonn": np.random.choice(list(gender_bins.keys()), num_samples),
            "Alder": np.random.randint(15, 67, num_samples),  # Ages between 15 and 66
            "syss_student": np.random.choice(["01", "02", "03", "04"], num_samples),
            "n": 1,
        }
    )

    cat_mappings = {
        "Alder": {
            "15-24": range(15, 25),
            "25-34": range(25, 35),
            "35-44": range(35, 45),
            "45-54": range(45, 55),
            "55-66": range(55, 67),
            "15-21": range(15, 22),
            "22-30": range(22, 31),
            "31-40": range(31, 41),
            "41-50": range(41, 51),
            "51-66": range(51, 67),
            "15-30": range(15, 31),
            "31-45": range(31, 46),
            "46-66": range(46, 67),
        },
        "syss_student": {
            "01-02": ["01", "02"],
            "03-04": ["03", "04"],
            "02": ["02"],
            "04": ["04"],
        },
    }

    totalcodes = {"Alder": "Total", "syss_student": "Total", "Kjonn": "Begge"}

    tbl = all_combos_agg_inclusive(
        synthetic_data,
        groupcols=["Kjonn"],
        category_mappings=cat_mappings,
        totalcodes=totalcodes,
        valuecols=["n"],
        aggargs={"n": "sum"},
        grand_total=True,
        keep_empty=True,
    )

    # add totals and kjønn to cat_mappings for comparison in loop
    cat_mappings["Alder"]["Total"] = range(0, 100)
    cat_mappings["syss_student"]["Total"] = ["01", "02", "03", "04"]
    cat_mappings["Kjonn"] = {
        "1": ["1"],
        "2": ["2"],
        "Begge": ["1", "2"],
    }

    cat_alder = cat_mappings["Alder"]
    cat_student = cat_mappings["syss_student"]
    cat_kjonn = cat_mappings["Kjonn"]

    for alder in cat_alder:
        for student in cat_student:
            for kjonn in cat_kjonn:
                query = synthetic_data.loc[
                    (synthetic_data["Alder"].isin(cat_alder[alder]))
                    & (synthetic_data["syss_student"].isin(cat_student[student]))
                    & (synthetic_data["Kjonn"].isin(cat_kjonn[kjonn])),
                    :,
                ]
                n_observed = query.shape[0]
                print(
                    f"alder: {alder}, student: {student}, kjonn: {kjonn}, n_observed: {n_observed}"
                )
                n_predicted = tbl.loc[
                    (tbl["Alder"] == alder)
                    & (tbl["syss_student"] == student)
                    & (tbl["Kjonn"] == kjonn),
                    "n",
                ].values[0]

                n_predicted = 0 if math.isnan(n_predicted) else n_predicted

                assert n_observed == n_predicted
