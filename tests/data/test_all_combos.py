import pandas as pd

from fagfunksjoner.data.pandas_combinations import all_combos_agg


def dataset1():
    return pd.DataFrame(
        {
            "sex": ["Mann", "Kvinne", "Mann", "Mann", "Kvinne", "Mann"],
            "age": ["40", "50", "60", "50", "60", "40"],
            "points": [100, 90, 80, 70, 60, 50],
        }
    )


def doc_data():
    return pd.DataFrame(
        {
            "alder": [20, 60, 33, 33, 20],
            "kommune": ["0301", "3001", "0301", "5401", "0301"],
            "kjonn": ["1", "2", "1", "2", "2"],
            "inntekt": [1000000, 120000, 220000, 550000, 50000],
            "formue": [25000, 50000, 33000, 44000, 90000],
        }
    )


def data_out():
    return pd.DataFrame(
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


def data_out_keep_empty():
    return pd.DataFrame(
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


def test_simple_sum_fillna():
    data_in = dataset1()
    result = all_combos_agg(
        df=data_in,
        groupcols=["sex", "age"],
        aggargs={"points": sum},
        fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
    )

    assert result.equals(data_out())


def test_fillna_dict_keep_empty():
    data_in = dataset1()
    result = all_combos_agg(
        df=data_in,
        groupcols=["sex", "age"],
        aggargs={"points": sum},
        fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
        keep_empty=True,
    )

    assert result.equals(data_out_keep_empty())


def test_agg_simplified():
    data_in = dataset1()
    result = all_combos_agg(
        df=data_in,
        groupcols=["sex", "age"],
        valuecols=["points"],
        fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
        keep_empty=True,
    )

    assert result.equals(data_out_keep_empty())


def test_agg_nospecs():
    data_in = dataset1()
    result = all_combos_agg(
        df=data_in,
        groupcols=["sex", "age"],
        fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
        keep_empty=True,
    )

    assert result.equals(data_out_keep_empty())


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
