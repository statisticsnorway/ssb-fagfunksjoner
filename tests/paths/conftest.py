from datetime import datetime

import pytest



@pytest.fixture
def testdata_get_latest_gcs_files() -> dict:
    parquetfiler = [
      "ssb-test-dapla-team-data-produkt/testdata1_p2023_v1.parquet",
      "ssb-test-dapla-team-data-produkt/testdata1_p2023_v2.parquet",
      "ssb-test-dapla-team-data-produkt/testdata1_p2023_v3.parquet",
      "ssb-test-dapla-team-data-produkt/testdata1_p2023_v4.parquet",
      "ssb-test-dapla-team-data-produkt/testdata2_p2023_v1.parquet",
      "ssb-test-dapla-team-data-produkt/testdata2_p2023_v2.parquet",
      "ssb-test-dapla-team-data-produkt/testdata2_p2023_v3.parquet",
    ]
    
    jsonfiler = [
        "ssb-test-dapla-team-data-produkt/testdata1_p2023_v1.json",
        "ssb-test-dapla-team-data-produkt/testdata1_p2023_v2.json",
        "ssb-test-dapla-team-data-produkt/testdata2_p2023_v1.json",
        "ssb-test-dapla-team-data-produkt/testdata2_p2023_v2.json",
        "ssb-test-dapla-team-data-produkt/testdata2_p2023_v3.json",
        "ssb-test-dapla-team-data-produkt/testdata2_p2023_v4.json",
        "ssb-test-dapla-team-data-produkt/testdata2_p2023_v5.json",
    ]
    
    datoer = [
        datetime(2024, 2, 10, 8, 5, 10, 500000),
        datetime(2024, 2, 11, 8, 5, 10, 500000),
        datetime(2024, 2, 12, 8, 5, 10, 500000),
        datetime(2024, 2, 12, 11, 5, 10, 500000),
        datetime(2024, 2, 14, 8, 5, 10, 500000),
        datetime(2024, 2, 15, 8, 5, 10, 500000),
        datetime(2024, 2, 15, 12, 5, 10, 500000),
    ]

    data = {}

    for i in range(len(parquetfiler)):
        p_fname = parquetfiler[i]
        j_fname = jsonfiler[i]
        dato = datoer[i]
        data[p_fname] = {
            'name': p_fname,
            'mtime': dato
        }
        data[j_fname] = {
            'name': j_fname,
            'mtime': dato
        }
    
    return data
