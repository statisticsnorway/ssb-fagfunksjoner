import pytest
from unittest.mock import patch
import pandas as pd
from fagfunksjoner.api.valuta import download_exchange_rates, parse_response, ValutaData

# Mock JSON response
mock_json = {
    "meta": {
        "id": "IREF990679",
        "prepared": "2024-08-01T07:57:42",
        "test": False,
        "datasetId": "8b6a035e-f1bd-4a49-8561-2cd3e8a3b2a4",
        "sender": {"id": "Unknown"},
        "receiver": {"id": "guest"},
        "links": [{
            "rel": "self",
            "href": "/data/EXR/A.SEK.NOK.SP?detail=full&endPeriod=2022-01-01&format=sdmx-json&locale=no&startPeriod=2021-01-01",
            "uri": "https://raw.githubusercontent.com/sdmx-twg/sdmx-json/develop/structure-message/tools/schemas/1.0/sdmx-json-structure-schema.json"
        }]
    },
    "data": {
        "dataSets": [{
            "links": [{
                "rel": "dataflow",
                "urn": "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=NB:EXR(1.0)"
            }],
            "reportingBegin": "2021-01-01T00:00:00",
            "reportingEnd": "2022-01-01T23:59:59",
            "action": "Information",
            "series": {
                "0:0:0:0": {
                    "attributes": [0, 0, 0, 0],
                    "observations": {
                        "0": ["100.19"],
                        "1": ["95.03"]
                    }
                }
            }
        }],
        "structure": {
            "links": [{
                "rel": "dataflow",
                "urn": "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=NB:EXR(1.0)"
            }, {
                "rel": "datastructure",
                "urn": "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=NB:DSD_EXR(1.0)"
            }],
            "name": "Valutakurser",
            "names": {"no": "Valutakurser"},
            "description": "Norges Banks valutakurser",
            "descriptions": {"no": "Norges Banks valutakurser"},
            "dimensions": {
                "dataset": [],
                "series": [{
                    "id": "FREQ",
                    "name": "Frekvens",
                    "description": "Tidsintervallet for observasjoner over en gitt tidsperiode.",
                    "keyPosition": 0,
                    "role": None,
                    "values": [{"id": "A", "name": "Årlig", "description": "Årlig"}]
                }, {
                    "id": "BASE_CUR",
                    "name": "Basisvaluta",
                    "description": "Første valuta i et valutakvoteringspar. Også kalt transaksjonsvalutaen.",
                    "keyPosition": 1,
                    "role": None,
                    "values": [{"id": "SEK", "name": "Svenske kroner"}]
                }, {
                    "id": "QUOTE_CUR",
                    "name": "Kvoteringsvaluta",
                    "description": "Den andre valutaen i et valutakvoteringspar. Også kjent som motvaluta.",
                    "keyPosition": 2,
                    "role": None,
                    "values": [{"id": "NOK", "name": "Norske kroner"}]
                }, {
                    "id": "TENOR",
                    "name": "Løpetid",
                    "description": "Mengde tid igjen inntil tilbakebetaling av et lån eller en finansiell kontrakt utløper.",
                    "keyPosition": 3,
                    "role": None,
                    "values": [{"id": "SP", "name": "Spot"}]
                }],
                "observation": [{
                    "id": "TIME_PERIOD",
                    "name": "Tidsperiode",
                    "description": "Tidsperioden eller tidspunktet for den målte observasjonen.",
                    "keyPosition": 4,
                    "role": "time",
                    "values": [{
                        "start": "2021-01-01T00:00:00",
                        "end": "2021-12-31T23:59:59",
                        "id": "2021",
                        "name": "2021"
                    }, {
                        "start": "2022-01-01T00:00:00",
                        "end": "2022-12-31T23:59:59",
                        "id": "2022",
                        "name": "2022"
                    }]
                }]
            },
            "attributes": {
                "dataset": [],
                "series": [{
                    "id": "DECIMALS",
                    "name": "Desimaler",
                    "description": "Antall sifre til høyre for desimalskilletegnet.",
                    "relationship": {"dimensions": ["BASE_CUR", "QUOTE_CUR", "TENOR"]},
                    "role": None,
                    "values": [{"id": "2", "name": "2"}]
                }, {
                    "id": "CALCULATED",
                    "name": "Kalkulert",
                    "description": "Indikerer om verdien er en kalkulasjon / anslag eller en observert verdi.",
                    "relationship": {"dimensions": ["BASE_CUR", "QUOTE_CUR", "TENOR"]},
                    "role": None,
                    "values": [{"id": "false", "name": "false"}]
                }, {
                    "id": "UNIT_MULT",
                    "name": "Multiplikator",
                    "description": "Eksponent i tiende potens slik at en multiplikasjon av observasjonsverdien med 10^UNIT_MULT gir verdien av en enhet.",
                    "relationship": {"dimensions": ["BASE_CUR", "QUOTE_CUR", "TENOR"]},
                    "role": None,
                    "values": [{"id": "2", "name": "Hundre"}]
                }, {
                    "id": "COLLECTION",
                    "name": "Innsamlingstidspunkt",
                    "description": "Datoer eller perioder for når verdien ble innhentet.",
                    "relationship": {"dimensions": ["BASE_CUR", "QUOTE_CUR", "TENOR"]},
                    "role": None,
                    "values": [{"id": "A", "name": "Gjennomsnitt av observasjoner gjennom perioden"}]
                }],
                "observation": []
            }
        }
    }
}

@pytest.fixture
def mock_response():
    """Mock the requests.get response."""
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = mock_json
        mock_get.return_value.raise_for_status = lambda: None
        yield mock_get

def test_download_exchange_rates(mock_response):
    """Test the download_exchange_rates function."""
    valuta_data = download_exchange_rates(
        frequency="A", currency="SEK", date_from="2021-01-01", date_to="2022-01-01"
    )
    
    assert isinstance(valuta_data, ValutaData)
    assert isinstance(valuta_data.df, pd.DataFrame)
    assert not valuta_data.df.empty

def test_parse_response():
    """Test the parse_response function."""
    valuta_data = parse_response(mock_json)
    
    assert isinstance(valuta_data, ValutaData)
    assert isinstance(valuta_data.df, pd.DataFrame)
    assert not valuta_data.df.empty
    assert set(valuta_data.df.columns).issuperset({
        'FREQ', 'BASE_CUR', 'QUOTE_CUR', 'TENOR', 'TIME_PERIOD', 
        'FREQ_id', 'BASE_CUR_id', 'QUOTE_CUR_id', 'TENOR_id', 'TIME_PERIOD_id',
        'Observation', 'DECIMALS', 'CALCULATED', 'UNIT_MULT', 'COLLECTION',
        'DECIMALS_id', 'CALCULATED_id', 'UNIT_MULT_id', 'COLLECTION_id'
    })