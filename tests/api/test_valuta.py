import pathlib

import pytest

from fagfunksjoner.api import valuta


@pytest.fixture
def example_data_path() -> pathlib.Path:
    return pathlib.Path(__file__).parent / "exchange_rates.csv"


def test_always_works() -> None:
    """A test that should always work."""
    assert True


def test_floats_compare_approximately_equal() -> None:
    """A test that compare float values."""
    assert 0.1 + 0.1 + 0.1 == pytest.approx(0.3)


def test_convert_to_dataframe(example_data_path) -> None:
    """Test that CSV data are correctly converted."""
    data = valuta.exchange_rates(example_data_path)
    assert len(data) == 2


@pytest.mark.needs_internet
def test_download_from_api() -> None:
    """Test that data can be downloaded from API."""
    valuta.download_exchange_rates(
        frequency="A", currency="SEK", date_from="2021-01-01", date_to="2022-01-01"
    )
