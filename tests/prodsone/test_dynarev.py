from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest

from fagfunksjoner.prodsone.dynarev import (
    dynarev_uttrekk,  # Replace with the actual import path of your function
)


@pytest.fixture
def mock_oracle():
    with patch("fagfunksjoner.prodsone.dynarev.Oracle") as mock_oracle_class:
        mock_oracle_instance = MagicMock()
        mock_oracle_class.return_value = mock_oracle_instance
        yield mock_oracle_instance


def test_dynarev_uttrekk_no_sfu_cols(mock_oracle):
    mock_oracle.select.side_effect = [
        pd.DataFrame(
            {
                "enhets_id": [1, 2],
                "enhets_type": ["BEDR", "BEDR"],
                "delreg_nr": [1, 1],
                "lopenr": [1, 2],
                "rad_nr": [0, 0],
                "felt_id": ["A", "B"],
                "felt_verdi": [10, 20],
            }
        )
    ]

    with patch("builtins.input", return_value="test_db"):
        result = dynarev_uttrekk(delreg_nr="1", skjema="test", sfu_cols=None)

    assert isinstance(result, pd.DataFrame)
    assert not isinstance(result, tuple)


def test_dynarev_uttrekk_with_sfu_cols(mock_oracle):
    mock_oracle.select.side_effect = [
        pd.DataFrame(
            {
                "enhets_id": [1, 2],
                "enhets_type": ["BEDR", "BEDR"],
                "delreg_nr": [1, 1],
                "lopenr": [1, 2],
                "rad_nr": [0, 0],
                "felt_id": ["A", "B"],
                "felt_verdi": [10, 20],
            }
        ),
        pd.DataFrame(
            {
                "col1": [1, 2],
                "col2": ["val1", "val2"],
            }
        ),
    ]

    with patch("builtins.input", return_value="test_db"):
        sfu_cols = ["col1", "col2"]
        result = dynarev_uttrekk(delreg_nr="1", skjema="test", sfu_cols=sfu_cols)

    assert isinstance(result, tuple)
    assert len(result) == 2
    assert set(result[1].columns) == set(sfu_cols)


def test_dynarev_uttrekk_dublettsjekk(mock_oracle):
    mock_oracle.select.side_effect = [
        pd.DataFrame(
            {
                "enhets_id": [1, 2],
                "enhets_type": ["BEDR", "BEDR"],
                "delreg_nr": [1, 1],
                "lopenr": [1, 2],
                "rad_nr": [0, 0],
                "felt_id": ["A", "B"],
                "felt_verdi": [10, 20],
            }
        ),
        pd.DataFrame(
            {
                "enhets_id": [1, 2],
                "antall_skjemaer": [2, 3],
            }
        ),
    ]

    with patch("builtins.input", return_value="test_db"):
        result = dynarev_uttrekk(delreg_nr="1", skjema="test", dublettsjekk=True)

    assert isinstance(result, tuple)
    assert len(result) == 2
    assert "antall_skjemaer" in result[1].columns
