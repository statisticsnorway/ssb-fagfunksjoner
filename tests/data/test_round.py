import pandas as pd

from src.fagfunksjoner.data.round import round_up


def test_round_up_float():
    """Test rounding a single float."""
    assert round_up(1.5, decimal_places=0) == 2
    assert round_up(2.5, decimal_places=0) == 3
    assert round_up(1.2345, decimal_places=2) == 1.23
    assert round_up(1.2355, decimal_places=2) == 1.24


def test_round_up_series():
    """Test rounding a pandas Series."""
    series = pd.Series([1.5, 2.5, 1.2345, 1.2355]).astype("Float64")
    rounded = round_up(series, decimal_places=0)
    expected = pd.Series([2, 3, 1, 1]).astype("Int64")
    pd.testing.assert_series_equal(rounded, expected)

    rounded = round_up(series, decimal_places=2)
    expected = pd.Series([1.50, 2.50, 1.23, 1.24]).astype("Float64")
    pd.testing.assert_series_equal(rounded, expected)


def test_round_up_dataframe_with_list():
    """Test rounding specific columns in a DataFrame using a list."""
    df = pd.DataFrame(
        {"col1": [1.5, 2.5, 1.2345, 1.2355], "col2": [3.5, 4.5, 5.6789, 6.7891]}
    ).astype({"col1": "Float64", "col2": "Float64"})
    rounded = round_up(df, decimal_places=0, col_names=["col1"])
    expected = pd.DataFrame(
        {"col1": [2, 3, 1, 1], "col2": [3.5, 4.5, 5.6789, 6.7891]}
    ).astype({"col1": "Int64", "col2": "Float64"})
    pd.testing.assert_frame_equal(rounded, expected)


def test_round_up_dataframe_with_dict():
    """Test rounding specific columns in a DataFrame using a dictionary."""
    df = pd.DataFrame(
        {"col1": [1.5, 2.5, 1.2345, 1.2355], "col2": [3.5, 4.5, 5.6789, 6.7891]}
    ).astype({"col1": "Float64", "col2": "Float64"})
    rounded = round_up(df, col_names={"col1": 0, "col2": 2})
    expected = pd.DataFrame(
        {"col1": [2, 3, 1, 1], "col2": [3.50, 4.50, 5.68, 6.79]}
    ).astype({"col1": "Int64", "col2": "Float64"})
    pd.testing.assert_frame_equal(rounded, expected)


def test_round_up_dataframe_with_string():
    """Test rounding a single column in a DataFrame using a string."""
    df = pd.DataFrame(
        {"col1": [1.5, 2.5, 1.2345, 1.2355], "col2": [3.5, 4.5, 5.6789, 6.7891]}
    ).astype({"col1": "Float64", "col2": "Float64"})
    rounded = round_up(df, decimal_places=0, col_names="col1")
    expected = pd.DataFrame(
        {"col1": [2, 3, 1, 1], "col2": [3.5, 4.5, 5.6789, 6.7891]}
    ).astype({"col1": "Int64", "col2": "Float64"})
    pd.testing.assert_frame_equal(rounded, expected)


def test_round_up_empty_dataframe():
    """Test rounding an empty DataFrame."""
    df = pd.DataFrame(columns=["col1", "col2"])
    rounded = round_up(df, decimal_places=0, col_names="col1")
    expected = pd.DataFrame(columns=["col1", "col2"]).astype({"col1": "Int64"})
    pd.testing.assert_frame_equal(rounded, expected)


def test_round_up_with_nan():
    """Test rounding with NaN values."""
    series = pd.Series([1.5, None, 2.5, float("nan")]).astype("Float64")
    rounded = round_up(series, decimal_places=0)
    expected = pd.Series([2, None, 3, None]).astype("Int64")
    pd.testing.assert_series_equal(rounded, expected)
