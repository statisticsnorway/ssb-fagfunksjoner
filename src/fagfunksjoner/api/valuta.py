from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import requests


@dataclass
class Link:
    """Represents a hyperlink related to the dataset.

    Args:
        rel (str): The relationship type of the link.
        href (str | None): The URL of the link (if available).
        uri (str | None): The URI of the link (if available).
        urn (str | None): The URN of the link (if available).
    """

    rel: str
    href: str | None = None
    uri: str | None = None
    urn: str | None = None


@dataclass
class Sender:
    """Represents the sender of the dataset.

    Args:
        id (str): The identifier of the sender.
    """

    id: str


@dataclass
class Receiver:
    """Represents the receiver of the dataset.

    Args:
        id (str): The identifier of the receiver.
    """

    id: str


@dataclass
class ValutaMeta:
    """Metadata related to the dataset.

    Args:
        id (str): The identifier of the metadata.
        prepared (str): The preparation timestamp of the metadata.
        test (bool): Indicates if the dataset is a test.
        datasetId (str): The identifier of the dataset.
        sender (Sender): The sender of the dataset.
        receiver (Receiver): The receiver of the dataset.
        links (list[Link]): A list of related links.
    """

    id: str
    prepared: str
    test: bool
    datasetId: str
    sender: Sender
    receiver: Receiver
    links: list[Link]


@dataclass
class Observation:
    """Represents an observation within the dataset.

    Args:
        id (str): The identifier of the observation.
        name (str): The name of the observation.
        description (str): The description of the observation.
        keyPosition (int): The key position of the observation in the dataset.
        role (str | None): The role of the observation (if any).
        values (list[dict[str, str | float]]): The values associated with the observation.
    """

    id: str
    name: str
    description: str
    keyPosition: int
    role: str | None
    values: list[dict[str, str | float]]


@dataclass
class Attribute:
    """Represents an attribute within the dataset.

    Args:
        id (str): The identifier of the attribute.
        name (str): The name of the attribute.
        description (str): The description of the attribute.
        relationship (dict[str, list[str]]): The relationship of the attribute to dimensions.
        role (str | None): The role of the attribute (if any).
        values (list[dict[str, str]]): The values associated with the attribute.
    """

    id: str
    name: str
    description: str
    relationship: dict[str, list[str]]
    role: str | None
    values: list[dict[str, str]]


@dataclass
class Dimension:
    """Represents a dimension within the dataset.

    Args:
        id (str): The identifier of the dimension.
        name (str): The name of the dimension.
        description (str): The description of the dimension.
        keyPosition (int): The key position of the dimension in the dataset.
        role (str | None): The role of the dimension (if any).
        values (list[dict[str, str]]): The values associated with the dimension.
    """

    id: str
    name: str
    description: str
    keyPosition: int
    role: str | None
    values: list[dict[str, str]]


@dataclass
class Structure:
    """Represents the structure of the dataset.

    Args:
        links (list[Link]): A list of related links.
        name (str): The name of the structure.
        names (dict[str, str]): A dictionary of names in different languages.
        description (str): The description of the structure.
        descriptions (dict[str, str]): A dictionary of descriptions in different languages.
        dimensions (dict[str, list[Dimension]]): The dimensions of the structure.
        attributes (dict[str, list[Attribute]]): The attributes of the structure.
    """

    links: list[Link]
    name: str
    names: dict[str, str]
    description: str
    descriptions: dict[str, str]
    dimensions: dict[str, list[Dimension]]
    attributes: dict[str, list[Attribute]]


@dataclass
class Series:
    """Represents a series within the dataset.

    Args:
        attributes (list[int]): The attributes of the series.
        observations (dict[str, list[str]]): The observations within the series.
    """

    attributes: list[int]
    observations: dict[str, list[str]]


@dataclass
class DataSet:
    """Represents a dataset.

    Args:
        links (list[Link]): A list of related links.
        reportingBegin (str): The start date of the reporting period.
        reportingEnd (str): The end date of the reporting period.
        action (str): The action associated with the dataset.
        series (dict[str, Series]): The series within the dataset.
    """

    links: list[Link]
    reportingBegin: str
    reportingEnd: str
    action: str
    series: dict[str, Series]


@dataclass
class Data:
    """Represents the data part of the dataset.

    Args:
        dataSets (list[DataSet]): A list of datasets.
        structure (Structure): The structure of the dataset.
    """

    dataSets: list[DataSet]
    structure: Structure


@dataclass
class ValutaData:
    """Represents the entire dataset including metadata and data.

    Args:
        meta (ValutaMeta): The metadata of the dataset.
        data (Data): The data part of the dataset.
        df (pd.DataFrame | None): A DataFrame representation of the dataset (optional).
    """

    meta: ValutaMeta
    data: Data
    df: pd.DataFrame | None = None


URL_NORGES_BANK = (
    "https://data.norges-bank.no/api/data/EXR/{frequency}.{currency}.NOK.SP?"
    "format=sdmx-json&startPeriod={date_from}&endPeriod={date_to}"
    "&locale={language}&detail={detail}"
)


def parse_response(json_data: dict[str, Any]) -> ValutaData:
    """Convert the json response to a ValutaData object with nested dataclasses.

    Args:
        json_data (dict[str, Any]): The response from the Norske Bank API.

    Returns:
        ValutaData: An instance of the dataclass ValutaData.
    """
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
        links=links_meta,
    )

    # Parsing Structure
    structure = data["structure"]
    structure_links = [Link(**link) for link in structure["links"]]
    dimensions: dict[str, list[Dimension]] = {
        k: [Dimension(**dim) for dim in v] for k, v in structure["dimensions"].items()
    }
    attributes = {
        k: [Attribute(**attr) for attr in v] for k, v in structure["attributes"].items()
    }
    structure_obj = Structure(
        links=structure_links,
        name=structure["name"],
        names=structure["names"],
        description=structure["description"],
        descriptions=structure["descriptions"],
        dimensions=dimensions,
        attributes=attributes,
    )

    # Parsing DataSets
    datasets = []
    for dataset in data["dataSets"]:
        dataset_links = [Link(**link) for link in dataset["links"]]
        series = {
            k: Series(attributes=v["attributes"], observations=v["observations"])
            for k, v in dataset["series"].items()
        }
        datasets.append(
            DataSet(
                links=dataset_links,
                reportingBegin=dataset["reportingBegin"],
                reportingEnd=dataset["reportingEnd"],
                action=dataset["action"],
                series=series,
            )
        )

    data_obj = Data(dataSets=datasets, structure=structure_obj)

    valuta_data = ValutaData(meta=valuta_meta, data=data_obj)

    # Create DataFrame
    dimension_keys = [dim.id for dim in structure_obj.dimensions.get("series", [])]
    observation_keys = [
        dim.id for dim in structure_obj.dimensions.get("observation", [])
    ]
    records = []
    for dataset_obj in data_obj.dataSets:
        for series_key, series_val in dataset_obj.series.items():
            for obs_key, obs_value in series_val.observations.items():
                record = {
                    **{
                        dim.id: dim.values[int(series_key.split(":")[dim.keyPosition])][
                            "name"
                        ]
                        for dim in structure_obj.dimensions.get("series", [])
                    },
                    **{
                        dim.id
                        + "_id": dim.values[
                            int(series_key.split(":")[dim.keyPosition])
                        ]["id"]
                        for dim in structure_obj.dimensions.get("series", [])
                    },
                    **{
                        dim.id: next(
                            (val["name"] for val in dim.values if val["id"] == obs_key),
                            obs_key,
                        )
                        for dim in structure_obj.dimensions.get("observation", [])
                    },
                    **{
                        dim.id + "_id": obs_key
                        for dim in structure_obj.dimensions.get("observation", [])
                    },
                    "Observation": obs_value[0],
                }
                # Add attribute information
                for _attr_key, attr_list in structure_obj.attributes.items():
                    for attr in attr_list:
                        attr_index = next(
                            (
                                i
                                for i, dim in enumerate(
                                    structure_obj.dimensions.get("series", [])
                                )
                                if dim.id == attr.relationship["dimensions"][0]
                            ),
                            None,
                        )
                        if attr_index is not None:
                            record[attr.id] = attr.values[
                                series_val.attributes[attr_index]
                            ]["name"]
                            record[attr.id + "_id"] = attr.values[
                                series_val.attributes[attr_index]
                            ]["id"]
                records.append(record)

    df = pd.DataFrame(
        records,
        columns=dimension_keys
        + [dim.id + "_id" for dim in structure_obj.dimensions.get("series", [])]
        + observation_keys
        + [dim.id + "_id" for dim in structure_obj.dimensions.get("observation", [])]
        + ["Observation"]
        + [
            attr.id
            for attr_list in structure_obj.attributes.values()
            for attr in attr_list
        ]
        + [
            attr.id + "_id"
            for attr_list in structure_obj.attributes.values()
            for attr in attr_list
        ],
    )
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
