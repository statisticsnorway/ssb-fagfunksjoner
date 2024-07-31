import json
import pandas as pd
from unittest import mock
from fagfunksjoner.data.pandas_dtypes import (
    dtype_set_from_json,
    dtype_store_json,
    auto_dtype,
    decode_bytes,
    object_to_strings,
    strings_to_int,
    smaller_ints,
    categories_threshold
)


def test_dtype_set_from_json():
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    json_dtypes = {
        "col1": {"dtype": "int32", "secondary_dtype": None},
        "col2": {"dtype": "category", "secondary_dtype": "object"}
    }

    with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(json_dtypes))):
        with mock.patch("json.load", return_value=json_dtypes):
            result_df = dtype_set_from_json(df, "dummy_path.json")
            assert result_df["col1"].dtype == "int32"
            assert result_df["col2"].dtype.name == "category"

def test_dtype_store_json():
    df = pd.DataFrame({'col1': pd.Categorical(['a', 'b']), 'col2': [1, 2]})
    json_path = "dummy_path.json"

    expected_json = {
        "col1": {"dtype": "category", "secondary_dtype": "object"},
        "col2": {"dtype": "int64", "secondary_dtype": None}
    }

    with mock.patch("builtins.open", mock.mock_open()) as mocked_file:
        dtype_store_json(df, json_path)
        mocked_file().write.assert_called_once_with(json.dumps(expected_json))

def test_auto_dtype():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
    result_df = auto_dtype(df, cardinality_threshold=2)
    assert result_df["a"].dtype.name == "Int8"  # Downcasted int
    assert result_df["b"].dtype.name == "string[pyarrow]"

def test_decode_bytes():
    df = pd.DataFrame({'A': [b'byte1', b'byte2'], 'B': ['str1', 'str2']})
    result_df = decode_bytes(df)
    assert result_df["a"].dtype.name == "string[pyarrow]"

def test_object_to_strings():
    df = pd.DataFrame({'A': [' a ', ' b '], 'B': [1, 2]})
    result_df = object_to_strings(df)
    assert result_df["a"].dtype.name == "string[pyarrow]"
    assert result_df["a"].iloc[0] == "a"

def test_strings_to_int():
    df = pd.DataFrame({'A': ['1', '2', '3'], 'B': ['01', '02', '03']})
    result_df = strings_to_int(df)
    assert result_df["a"].dtype.name == "Int64"
    assert result_df["b"].dtype.name == "string[pyarrow]"  # Leading zeros prevent conversion

def test_smaller_ints():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [1000, 2000, 3000]})
    df["A"] = df["A"].astype("Int64")
    df["B"] = df["B"].astype("Int64")
    result_df = smaller_ints(df)
    assert result_df["a"].dtype.name == "int8"  # Downcasted to smallest int
    assert result_df["b"].dtype.name == "int16"

def test_categories_threshold():
    df = pd.DataFrame({'A': ['a', 'b', 'c'], 'B': ['x', 'x', 'y']})
    result_df = categories_threshold(df, cardinality_threshold=2)
    assert result_df["b"].dtype.name == "category"
    assert result_df["a"].dtype.name == "string[pyarrow]"