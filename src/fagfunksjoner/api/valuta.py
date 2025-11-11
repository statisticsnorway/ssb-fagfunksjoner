import datetime
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests

from fagfunksjoner.fagfunksjoner_logger import logger


@dataclass
class Link:
    """Represents a hyperlink related to the dataset.

    Args:
        rel: The relationship type of the link.
        href: The URL of the link (if available).
        uri: The URI of the link (if available).
        urn: The URN of the link (if available).
    """

    rel: str
    href: str | None = None
    uri: str | None = None
    urn: str | None = None


@dataclass
class Sender:
    """Represents the sender of the dataset.

    Args:
        id: The identifier of the sender.
    """

    id: str


@dataclass
class Receiver:
    """Represents the receiver of the dataset.

    Args:
        id: The identifier of the receiver.
    """

    id: str


@dataclass
class ValutaMeta:
    """Metadata related to the dataset.

    Args:
        id: The identifier of the metadata.
        prepared: The preparation timestamp of the metadata.
        test: Indicates if the dataset is a test.
        datasetId: The identifier of the dataset.
        sender: The sender of the dataset.
        receiver : The receiver of the dataset.
        links: A list of related links.
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
        id: The identifier of the observation.
        name: The name of the observation.
        description: The description of the observation.
        keyPosition: The key position of the observation in the dataset.
        role: The role of the observation (if any).
        values: The values associated with the observation.
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
        id: The identifier of the attribute.
        name: The name of the attribute.
        description: The description of the attribute.
        relationship: The relationship of the attribute to dimensions.
        role: The role of the attribute (if any).
        values: The values associated with the attribute.
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
        id: The identifier of the dimension.
        name: The name of the dimension.
        description: The description of the dimension.
        keyPosition: The key position of the dimension in the dataset.
        role: The role of the dimension (if any).
        values: The values associated with the dimension.
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
        links: A list of related links.
        name: The name of the structure.
        names: A dictionary of names in different languages.
        description: The description of the structure.
        descriptions: A dictionary of descriptions in different languages.
        dimensions: The dimensions of the structure.
        attributes: The attributes of the structure.
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
        attributes: The attributes of the series.
        observations: The observations within the series.
    """

    attributes: list[int]
    observations: dict[str, list[str]]


@dataclass
class DataSet:
    """Represents a dataset.

    Args:
        links: A list of related links.
        reportingBegin: The start date of the reporting period.
        reportingEnd: The end date of the reporting period.
        action: The action associated with the dataset.
        series: The series within the dataset.
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
        dataSets: A list of datasets.
        structure: The structure of the dataset.
    """

    dataSets: list[DataSet]
    structure: Structure


@dataclass
class ValutaData:
    """Represents the entire dataset including metadata and data.

    Args:
        meta: The metadata of the dataset.
        data: The data part of the dataset.
        df: A DataFrame representation of the dataset (optional).
    """

    meta: ValutaMeta
    data: Data
    df: pd.DataFrame | None = None


URL_NORGES_BANK = (
    "https://data.norges-bank.no/api/data/EXR/{frequency}.{currency}.NOK.SP?"
    "format=sdmx-json&startPeriod={date_from}&endPeriod={date_to}"
    "&locale={language}&detail={detail}"
)


def parse_structure(structure: dict[str, Any]) -> Structure:
    """Parse the structure section from data.

    Args:
        structure: Data containing the structure information.

    Returns:
        Structure: An instance of the Structure dataclass.
    """
    structure_links = [Link(**link) for link in structure["links"]]
    dimensions = {
        k: [Dimension(**dim) for dim in v] for k, v in structure["dimensions"].items()
    }
    attributes = {
        k: [Attribute(**attr) for attr in v] for k, v in structure["attributes"].items()
    }

    return Structure(
        links=structure_links,
        name=structure["name"],
        names=structure["names"],
        description=structure["description"],
        descriptions=structure["descriptions"],
        dimensions=dimensions,
        attributes=attributes,
    )


def parse_datasets(datasets_data: list[dict[str, Any]]) -> list[DataSet]:
    """Parse the datasets section from data.

    Args:
        datasets_data: Data containing datasets information.

    Returns:
        list[DataSet]: A list of DataSet dataclass instances.
    """
    datasets = []
    for dataset in datasets_data:
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
    return datasets


_PERIOD_WARNING_EMITTED = False


def create_dataframe(data_obj: Data, structure_obj: Structure) -> pd.DataFrame:
    """Create a DataFrame from data and structure objects.

    Args:
        data_obj: The data object containing datasets.
        structure_obj: The structure object containing dimensions and attributes.

    Returns:
        pd.DataFrame: A pandas DataFrame created from the data and structure objects.
    """
    global _PERIOD_WARNING_EMITTED
    if not _PERIOD_WARNING_EMITTED:
        logger.warning(
            "TIME_PERIOD fields are now returned as strings without implicit date parsing. "
            "Cast to datetime/period explicitly if needed."
        )
        _PERIOD_WARNING_EMITTED = True

    records = []

    for dataset_obj in data_obj.dataSets:
        for series_key, series_val in dataset_obj.series.items():
            for obs_key, obs_value in series_val.observations.items():
                records.append(
                    make_single_dataframe_record(
                        series_key, series_val, obs_key, obs_value, structure_obj
                    )
                )
    return pd.DataFrame(records)


def make_single_dataframe_record(
    series_key: str,
    series_val: Series,
    obs_key: str,
    obs_value: list[str],
    structure_obj: Structure,
) -> dict[str, str | float]:
    """Create a single record for a pandas DataFrame from a series and observation.

    This function generates a dictionary representing a single row in a pandas
    DataFrame based on the series key, series value, observation key, observation
    value, and the structure object of the dataset.

    Args:
        series_key: The key representing the series in the dataset.
        series_val: The series object containing attributes and observations.
        obs_key: The key representing the specific observation in the series.
        obs_value: The list of values corresponding to the observation.
        structure_obj: The structure object containing dataset dimensions and attributes.

    Returns:
        dict[str, str | float]: A dictionary representing a single record in the DataFrame.
    """
    observation_fields: dict[str, str] = {}
    for dim in structure_obj.dimensions.get("observation", []):
        observation: dict[str, str] = next(
            (val for i, val in enumerate(dim.values) if i == int(obs_key))
        )
        observation_fields |= {
            f"{dim.id}_{field}": val for field, val in observation.items()
        }

    record: dict[str, str | float] = {
        **{
            dim.id: dim.values[int(series_key.split(":")[dim.keyPosition])]["name"]
            for dim in structure_obj.dimensions.get("series", [])
        },
        **{
            dim.id
            + "_id": dim.values[int(series_key.split(":")[dim.keyPosition])]["id"]
            for dim in structure_obj.dimensions.get("series", [])
        },
        **observation_fields,
        "Observation": obs_value[0],
    }
    # Map series-level attributes by their declared order in structure.attributes['series']
    for ai, attr in enumerate(structure_obj.attributes.get("series", [])):
        # Guard against malformed input where series provides fewer indices
        idx = series_val.attributes[ai] if ai < len(series_val.attributes) else 0
        # Clamp to available values for the attribute
        if attr.values:
            idx = min(idx, len(attr.values) - 1)
            record[attr.id] = attr.values[idx]["name"]
            record[attr.id + "_id"] = attr.values[idx]["id"]
    return record


def parse_response(json_data: dict[str, Any]) -> ValutaData:
    """Convert the json response to a ValutaData object with nested dataclasses.

    Args:
        json_data: The response from the Norske Bank API.

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
    structure_obj = parse_structure(data["structure"])

    # Parsing DataSets
    datasets = parse_datasets(data["dataSets"])
    data_obj = Data(dataSets=datasets, structure=structure_obj)

    # Create DataFrame
    df = create_dataframe(data_obj, structure_obj)

    valuta_data = ValutaData(meta=valuta_meta, data=data_obj, df=df)

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
        currency: Specified in UPPER case letters. For multiple currencies, use a
            plus sign (e.g., 'GBP+EUR+USD'). No value gives all currencies.
        frequency: Can be B (Business, daily rates), M (monthly rates),
            A (annual rates). For multiple frequencies, use a plus sign
            (e.g., 'A+M'). No value gives all frequencies. For annual rates,
            the time interval must cover a full year, similarly for months.
        date_from: Specified in the format YYYY-MM-DD.
        date_to: Specified in the format YYYY-MM-DD. If None, defaults to today's date.
        language: 'no' for Norwegian, 'en' for English.
        detail: 'full' gives both data and attributes, 'dataonly' gives only data,
            'serieskeysonly' gives series without data or attributes,
            'nodata' gives series and attributes without data.

    Returns:
        ValutaData: The data retrieved from the API, parsed into a ValutaData object.
    """
    if date_to is None:
        date_to = datetime.date.today().strftime("%Y-%m-%d")

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
