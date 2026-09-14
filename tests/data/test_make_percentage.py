import pandas as pd
from pandas.testing import assert_series_equal


def test_make_percentage():
    df = pd.DataFrame(
        {
            "kjoenn": ["1", "2", "Total"],
            "invkat": ["A", "A", "A"],
            "antall": [40, 60, 100],
            "level": [0, 0, 1],
            "ways": [0, 0, 1],
        }
    )

    fillna_dict = {
        "kjoenn": "Total",
        "invkat": "Total",
    }

    result = make_percentage(
        df=df,
        percent_col="kjoenn",
        fillna_dict=fillna_dict,
    )

    expected = pd.Series([40.0, 60.0, 100.0], name="andel")

    assert_series_equal(
        result["andel"],
        expected,
        check_dtype=False,
    )

def test_make_percentage_multiple_groups():
    df = pd.DataFrame(
        {
            "kjoenn": ["1", "2", "Total", "1", "2", "Total"],
            "invkat": ["A", "A", "A", "B", "B", "B"],
            "antall": [40, 60, 100, 10, 30, 40],
            "level": [0, 0, 1, 0, 0, 1],
            "ways": [0, 0, 1, 0, 0, 1],
        }
    )

    fillna_dict = {
        "kjoenn": "Total",
        "invkat": "Total",
    }

    result = make_percentage(
        df=df,
        percent_col="kjoenn",
        fillna_dict=fillna_dict,
    )

    expected = [40.0, 60.0, 100.0, 25.0, 75.0, 100.0]

    assert result["andel"].tolist() == expected


def test_make_percentage_rounding():
    df = pd.DataFrame(
        {
            "kjoenn": ["1", "2", "Total"],
            "invkat": ["A", "A", "A"],
            "antall": [1, 2, 3],
            "level": [0, 0, 1],
            "ways": [0, 0, 1],
        }
    )

    fillna_dict = {"kjoenn": "Total"}

    result = make_percentage(
        df=df,
        percent_col="kjoenn",
        fillna_dict=fillna_dict,
        decimals=1,
    )

    assert result["andel"].tolist() == [33.3, 66.7, 100.0]

