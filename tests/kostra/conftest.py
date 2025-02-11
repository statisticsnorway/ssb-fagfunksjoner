from unittest.mock import patch

import pandas as pd
import pytest


@pytest.fixture
def mock_klass_classification():
    with patch(
        "fagfunksjoner.kostra.kostra_kommunekorr.KlassClassification"
    ) as MockKlassClassification:
        instance = MockKlassClassification.return_value
        instance.get_codes.return_value.data = pd.DataFrame(
            [
                {"code": "0301", "name": "Oslo"},
                {"code": "1103", "name": "Stavanger"},
                {"code": "3401", "name": "Kongsvinger"},
                {"code": "3405", "name": "Lillehammer"},
                {"code": "4204", "name": "Kristiansand"},
                {"code": "4601", "name": "Bergen"},
            ]
        )
        yield MockKlassClassification


@pytest.fixture
def mock_klass_correspondence():
    MockKlassCorrespondence = patch(
        "fagfunksjoner.kostra.kostra_kommunekorr.KlassCorrespondence"
    )

    def mock_init(
        self, source_classification_id, target_classification_id, from_date, to_date
    ):
        if target_classification_id == "112":
            self.data = pd.DataFrame(
                [
                    {
                        "sourceCode": "0301",
                        "sourceName": "Oslo",
                        "sourceShortName": "",
                        "targetCode": "EKG13",
                        "targetName": "KOSTRA-gruppe 13",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "1103",
                        "sourceName": "Stavanger",
                        "sourceShortName": "",
                        "targetCode": "EKG12",
                        "targetName": "KOSTRA-gruppe 12",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "3401",
                        "sourceName": "Kongsvinger",
                        "sourceShortName": "",
                        "targetCode": "EKG07",
                        "targetName": "KOSTRA-gruppe 7",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "3405",
                        "sourceName": "Lillehammer",
                        "sourceShortName": "",
                        "targetCode": "EKG09",
                        "targetName": "KOSTRA-gruppe 9",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "4204",
                        "sourceName": "Kristiansand",
                        "sourceShortName": "",
                        "targetCode": "EKG12",
                        "targetName": "KOSTRA-gruppe 12",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "4601",
                        "sourceName": "Bergen",
                        "sourceShortName": "",
                        "targetCode": "EKG12",
                        "targetName": "KOSTRA-gruppe 12",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                ]
            )
        elif target_classification_id == "104":
            self.data = pd.DataFrame(
                [
                    {
                        "sourceCode": "0301",
                        "sourceName": "Oslo",
                        "sourceShortName": "",
                        "targetCode": "03",
                        "targetName": "Oslo",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "1103",
                        "sourceName": "Stavanger",
                        "sourceShortName": "",
                        "targetCode": "11",
                        "targetName": "Rogaland",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "3401",
                        "sourceName": "Kongsvinger",
                        "sourceShortName": "",
                        "targetCode": "34",
                        "targetName": "Innlandet",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "3405",
                        "sourceName": "Lillehammer",
                        "sourceShortName": "",
                        "targetCode": "34",
                        "targetName": "Innlandet",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "4204",
                        "sourceName": "Kristiansand",
                        "sourceShortName": "",
                        "targetCode": "42",
                        "targetName": "Agder",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                    {
                        "sourceCode": "4601",
                        "sourceName": "Bergen",
                        "sourceShortName": "",
                        "targetCode": "46",
                        "targetName": "Vestland",
                        "targetShortName": "",
                        "validFrom": "2023-01-01",
                        "validTo": "2023-12-31",
                    },
                ]
            )

    MockKlassCorrespondence.side_effect = mock_init
    return MockKlassCorrespondence
