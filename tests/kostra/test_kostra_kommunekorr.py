import pandas as pd

from fagfunksjoner.kostra import kostra_kommunekorr


def test_kostra_kommunekorr(mock_klass_classification, mock_klass_correspondence):
    year = 2023
    result = kostra_kommunekorr(year)
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 7

    exp_cols = [
        "komnr",
        "komnavn",
        "fylknr",
        "fylknavn",
        "fylknr_eka",
        "fylknr_eka_m_tekst",
        "fylk_validFrom",
        "fylk_validTo",
        "kostragr",
        "kostragrnavn",
        "kostra_validFrom",
        "kostra_validTo",
        "landet",
        "landet_u_oslo",
    ]
    assert result.shape[1] == len(exp_cols)

    for c1, c2 in zip(exp_cols, result.columns, strict=False):
        assert c1 == c2
