import datetime
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from xml.etree import ElementTree as ET

import dateutil.parser
import requests as rs


@dataclass
class PublishingSpecifics:
    navn: str
    id: str
    statistikk: str
    variant: str
    status: str
    erPeriode: bool
    periodeFra: datetime.datetime
    periodeTil: datetime.datetime
    presisjon: str
    tidspunkt: datetime.datetime
    erEndret: bool
    deskFlyt: str
    endret: datetime.datetime
    erAvlyst: bool
    revisjon: str
    tittel: str


@dataclass
class Specifics:
    """Holds """
    publisering: PublishingSpecifics


@dataclass
class StatisticPublishing:
    """Top-level metadata for a specific statistical product."""
    id: str
    variant: str
    deskFlyt: str
    endret: datetime.datetime
    statistikkKortnavn: str
    specifics: Specifics


@dataclass
class MultiplePublishings:
    """Contains multiple statisticss, like when getting all the data in the API."""
    publisering: list[StatisticPublishing]
    antall: int
    dato: str


def kwargs_specifics(nested: dict[str, Any]) -> dict[str, Any]:
    """Map fields in specifics to kwargs for the dataclass.

    Args:
        nested (dict[str, Any]): The XML-datastructure to map.

    Returns:
        dict[str, Any]: Cleaned up data-structure
    """
    return {
        "navn": nested["publisering"]["navn"],
        "id": nested["publisering"]["@id"],
        "statistikk": nested["publisering"]["@statistikk"],
        "variant": nested["publisering"]["@variant"],
        "status": nested["publisering"]["@status"],
        "erPeriode": nested["publisering"]["@erPeriode"] == "true",
        "periodeFra": dateutil.parser.parse(nested["publisering"]["@periodeFra"]),
        "periodeTil": dateutil.parser.parse(nested["publisering"]["@periodeTil"]),
        "presisjon": nested["publisering"]["@presisjon"],
        "tidspunkt": dateutil.parser.parse(nested["publisering"]["@tidspunkt"]),
        "erEndret": nested["publisering"]["@erEndret"] == "true",
        "deskFlyt": nested["publisering"]["@deskFlyt"],
        "endret": dateutil.parser.parse(nested["publisering"]["@endret"]),
        "erAvlyst": nested["publisering"]["@erAvlyst"] == "true",
        "revisjon": nested["publisering"]["@revisjon"],
        "tittel": nested["publisering"]["@tittel"],
    }


@lru_cache(maxsize=1)
def get_statistics_register() -> list[dict[str, Any]]:
    """Get the overview of all the statistical products from the API.

    Returns:
        dict[str, Any]: The summary of all the products.
    """
    response = rs.get("https://i.ssb.no/statistikkregisteret/statistics")
    response.raise_for_status()
    stats: list[dict[str, Any]] = response.json()["statistics"]
    return stats


def find_stat_shortcode(
    shortcode_or_id: str = "trosamf",
    get_singles: bool = True,
    get_publishings: bool = True,
    get_publishing_specifics: bool = True,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Find the data for a statistical product by searching by its shortname.

    Args:
        shortcode_or_id (str): The shortname for the statistical product. Defaults to "trosamf".
        get_singles (bool): Get more single data. Defaults to True.
        get_publishings (bool): Get more publishing data. Defaults to True.
        get_publishing_specifics (bool): Get the specific publishings data as well. Defaults to True.

    Returns:
        list[dict[str, Any]]: A data structure containing the found data on the product.
    """
    register = get_statistics_register()
    results = []
    for stat in register:
        if shortcode_or_id.isdigit() and shortcode_or_id == stat["id"]:
            if get_singles:
                stat["product_info"] = single_stat_xml(shortcode_or_id)
            if get_publishings:
                stat["publishings"] = find_publishings(
                    stat["shortName"], get_publishing_specifics
                )
            return stat
        elif shortcode_or_id in stat["shortName"]:
            if get_singles:
                stat["product_info"] = single_stat_xml(stat["id"])
            if get_publishings:
                stat["publishings"] = find_publishings(
                    stat["shortName"], get_publishing_specifics
                )
            results.append(stat)
    return results


@lru_cache(maxsize=128)
def single_stat_xml(stat_id: str = "4922") -> StatisticPublishing:
    """Get the metadata for specific product.

    Args:
        stat_id (str): The ID for the product in statistikkregisteret. Defaults to "4922".

    Returns:
        StatisticPublishing: Datastructure with the found metadata.
    """
    url = f"https://i.ssb.no/statistikkregisteret/statistikk/xml/{stat_id}"
    result = rs.get(url)
    result.raise_for_status()
    nested: dict[str, Any] = etree_to_dict(ET.fromstring(result.text))["statistikk"]
    return StatisticPublishing(
        id=nested["@id"],
        variant=nested["@variant"],
        deskFlyt=nested["@deskFlyt"],
        endret=dateutil.parser.parse(nested["@endret"]),
        statistikkKortnavn=nested["@statistikkKortnavn"],
        specifics=Specifics(
            publisering=PublishingSpecifics(**kwargs_specifics(nested["specifics"]))
        ),
    )


@lru_cache(maxsize=128)
def find_publishings(
    shortname: str = "trosamf", get_publishing_specifics: bool = True
) -> MultiplePublishings:
    """Get the publishings for a specific shortcode.

    Args:
        shortname (str): The shortcode to look for in the API among the publishings. Defaults to "trosamf".
        get_publishing_specifics (bool): Looks up more info about each of the publishings found. Defaults to True.

    Returns:
        MultiplePublishings: A datastructure with the found metadata about the statistics.
    """
    url = f"https://i.ssb.no/statistikkregisteret/publisering/listKortnavnSomXml?kortnavn={shortname}"
    result = rs.get(url)
    result.raise_for_status()
    publishings: dict[str, Any] = etree_to_dict(ET.fromstring(result.text))[
        "publiseringer"
    ]

    if not isinstance(publishings["publisering"], list):
        publishings["publisering"] = [publishings["publisering"]]

    if get_publishing_specifics:
        for publish in publishings["publisering"]:
            publish["specifics"] = specific_publishing(publish["@id"])

    return MultiplePublishings(
        publisering=[
            StatisticPublishing(
                id=pub["@id"],
                variant=pub["@variant"],
                deskFlyt=pub["@deskFlyt"],
                endret=dateutil.parser.parse(pub["@endret"]),
                statistikkKortnavn=pub["@statistikkKortnavn"],
                specifics=Specifics(
                    publisering=PublishingSpecifics(**kwargs_specifics(pub["specifics"]))
                ),
            )
            for pub in publishings["publisering"]
        ],
        antall=int(publishings["@antall"]),
        dato=publishings["@dato"],
    )


def time_until_publishing(shortname: str = "trosamf") -> datetime.timedelta | None:
    """Calculate the time between now and the publishing.

    Returns a negative timedelta, if there is no future publishing recorded.

    Args:
        shortname (str): The shortcode to look for in the API among the publishings. Defaults to "trosamf".

    Returns:
        datetime.timedelta | None : The time difference between now, and the latest publishing date.
            If no publishingdata is found, returns None.
    """
    pub = find_latest_publishing(shortname)
    if pub is not None:
        return pub.specifics.publisering.tidspunkt - datetime.datetime.now()
    return None


def find_latest_publishing(shortname: str = "trosamf") -> PublishingSpecifics | None:
    """Find the date of the latest publishing of the statistical product.

    Args:
        shortname (str): The shortname to find the latest publishing for. Defaults to "trosamf".

    Returns:
        PublishingSpecifics | None: data about the specific publishing. Or None if nothing is found.
    """
    max_date = dateutil.parser.parse("2000-01-01")
    max_publ: StatisticPublishing | None = None
    for pub in find_publishings(shortname).publisering:
        current_date = pub.specifics.publisering.tidspunkt
        if current_date > max_date:
            max_publ = pub
            max_date = current_date
    return max_publ


@lru_cache(maxsize=128)
def specific_publishing(publish_id: str = "162143") -> Specifics:
    """Get the publishing-data from a specific publishing-ID in statistikkregisteret.

    Args:
        publish_id (str): The API-ID for the publishing. Defaults to "162143".

    Returns:
        Specifics: The metadata found for the specific publishing.
    """
    url = f"https://i.ssb.no/statistikkregisteret/publisering/xml/{publish_id}"
    result = rs.get(url)
    result.raise_for_status()
    nested: dict[str, Any] = etree_to_dict(ET.fromstring(result.text))["publisering"]
    return Specifics(
        publisering=PublishingSpecifics(**kwargs_specifics(nested["specifics"]))
    )


def etree_to_dict(t: ET.Element) -> dict[str, Any]:
    """Convert an XML-tree to a python dictionary.

    Args:
        t (ET): The XML element to convert.

    Returns:
        dict[str, Any]: The python dictionary that has been converted to.
    """
    d: dict[str, Any] = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(("@" + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag]["#text"] = text
        else:
            d[t.tag] = text
    return d
