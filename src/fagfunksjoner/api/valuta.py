import requests
from datetime import date
from typing import Any
from dataclasses import dataclass
import pandas as pd

@dataclass
class Link:
    rel: str
    href: str | None = None
    uri: str | None = None
    urn: str | None = None

@dataclass
class Sender:
    id: str

@dataclass
class Receiver:
    id: str

@dataclass
class ValutaMeta:
    id: str
    prepared: str
    test: bool
    datasetId: str
    sender: Sender
    receiver: Receiver
    links: list[Link]

@dataclass
class Observation:
    id: str
    name: str
    description: str
    keyPosition: int
    role: str | None
    values: list[dict[str, str | float]]

@dataclass
class Attribute:
    id: str
    name: str
    description: str
    relationship: dict[str, list[str]]
    role: str | None
    values: list[dict[str, str]]

@dataclass
class Dimension:
    id: str
    name: str
    description: str
    keyPosition: int
    role: str | None
    values: list[dict[str, str]]

@dataclass
class Structure:
    links: list[Link]
    name: str
    names: dict[str, str]
    description: str
    descriptions: dict[str, str]
    dimensions: dict[str, list[Dimension | Observation]]
    attributes: dict[str, list[Attribute]]

@dataclass
class Series:
    attributes: list[int]
    observations: dict[str, list[str]]

@dataclass
class DataSet:
    links: list[Link]
    reportingBegin: str
    reportingEnd: str
    action: str
    series: dict[str, Series]

@dataclass
class Data:
    dataSets: list[DataSet]
    structure: Structure

@dataclass
class ValutaData:
    meta: ValutaMeta
    data: Data
    df: pd.DataFrame | None = None

URL_NORGES_BANK = (
    "https://data.norges-bank.no/api/data/EXR/{frequency}.{currency}.NOK.SP?"
    "format=sdmx-json&startPeriod={date_from}&endPeriod={date_to}"
    "&locale={language}&detail={detail}"
)

def parse_response(json_data: dict[str, Any]) -> ValutaData:
    meta = json_data["meta"]
    data = json_data["data"]

    # Parsing ValutaMeta
    sender = Sender(**meta["sender"])
    receiver = Receiver(**meta["receiver"])
    links_meta = [Link(**link) for link in meta["links"]]
    valuta_meta = ValutaMeta(
        id=meta["id"],
        prepared=meta["prepared"],
        test=meta["test"],
        datasetId=meta["datasetId"],
        sender=sender,
        receiver=receiver,
        links=links_meta
    )

    # Parsing Structure
    structure = data["structure"]
    structure_links = [Link(**link) for link in structure["links"]]
    dimensions = {k: [Dimension(**dim) for dim in v] for k, v in structure["dimensions"].items()}
    attributes = {k: [Attribute(**attr) for attr in v] for k, v in structure["attributes"].items()}
    structure_obj = Structure(
        links=structure_links,
        name=structure["name"],
        names=structure["names"],
        description=structure["description"],
        descriptions=structure["descriptions"],
        dimensions=dimensions,
        attributes=attributes
    )

    # Parsing DataSets
    datasets = []
    for dataset in data["dataSets"]:
        dataset_links = [Link(**link) for link in dataset["links"]]
        series = {k: Series(attributes=v["attributes"], observations=v["observations"]) for k, v in dataset["series"].items()}
        datasets.append(DataSet(
            links=dataset_links,
            reportingBegin=dataset["reportingBegin"],
            reportingEnd=dataset["reportingEnd"],
            action=dataset["action"],
            series=series
        ))

    data_obj = Data(
        dataSets=datasets,
        structure=structure_obj
    )

    valuta_data = ValutaData(meta=valuta_meta, data=data_obj)

    # Create DataFrame
    dimension_keys = [dim.id for dim in structure_obj.dimensions['series']]
    observation_keys = [dim.id for dim in structure_obj.dimensions['observation']]
    records = []
    for dataset in data_obj.dataSets:
        for series_key, series in dataset.series.items():
            for obs_key, obs_value in series.observations.items():
                record = {
                    **{dim.id: dim.values[int(series_key.split(':')[dim.keyPosition])]['name'] for dim in structure_obj.dimensions['series']},
                    **{dim.id + '_id': dim.values[int(series_key.split(':')[dim.keyPosition])]['id'] for dim in structure_obj.dimensions['series']},
                    **{dim.id: next((val['name'] for val in dim.values if val['id'] == obs_key), obs_key) for dim in structure_obj.dimensions['observation']},
                    **{dim.id + '_id': obs_key for dim in structure_obj.dimensions['observation']},
                    'Observation': obs_value[0]
                }
                # Add attribute information
                for attr_key, attr_list in structure_obj.attributes.items():
                    for attr in attr_list:
                        attr_index = structure_obj.dimensions['series'].index(next(filter(lambda x: x.id == attr.relationship['dimensions'][0], structure_obj.dimensions['series'])))
                        record[attr.id] = attr.values[series.attributes[attr_index]]['name']
                        record[attr.id + '_id'] = attr.values[series.attributes[attr_index]]['id']
                records.append(record)
    
    df = pd.DataFrame(records, columns=dimension_keys + [dim.id + '_id' for dim in structure_obj.dimensions['series']] + observation_keys + [dim.id + '_id' for dim in structure_obj.dimensions['observation']] + ['Observation'] + [attr.id for attr_list in structure_obj.attributes.values() for attr in attr_list] + [attr.id + '_id' for attr_list in structure_obj.attributes.values() for attr in attr_list])
    valuta_data.df = df

    return valuta_data

def download_exchange_rates(
    currency: str = "",
    frequency: str = "M",
    date_from: str = "2021-01-01",
    date_to: None | str = None,
    language: str = "no",
    detail: str = "full",
) -> ValutaData:
    """Fetch exchange rates from Norges Bank's API.

    See https://app.norges-bank.no/query/index.html#/no/

    Args:
        currency (str): Specified in UPPER case letters. For multiple currencies, use a
            plus sign (e.g., 'GBP+EUR+USD'). No value gives all currencies.
        frequency (str): Can be B (Business, daily rates), M (monthly rates),
            A (annual rates). For multiple frequencies, use a plus sign
            (e.g., 'A+M'). No value gives all frequencies. For annual rates,
            the time interval must cover a full year, similarly for months.
        date_from (str): Specified in the format YYYY-MM-DD.
        date_to (str | None): Specified in the format YYYY-MM-DD. If None, defaults to today's date.
        language (str): 'no' for Norwegian, 'en' for English.
        detail (str): 'full' gives both data and attributes, 'dataonly' gives only data,
            'serieskeysonly' gives series without data or attributes,
            'nodata' gives series and attributes without data.

    Returns:
        ValutaData: The data retrieved from the API, parsed into a ValutaData object.
    """
    if date_to is None:
        date_to = date.today().strftime("%Y-%m-%d")

    url = URL_NORGES_BANK.format(
        frequency=frequency,
        currency=currency,
        date_from=date_from,
        date_to=date_to,
        language=language,
        detail=detail,
    )
    print(url)
    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()

    return parse_response(json_data)
