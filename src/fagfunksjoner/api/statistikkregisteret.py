import json
from collections import defaultdict
from functools import lru_cache
from xml.etree import ElementTree as ET

import dateutil
import requests as rs

from typing import Any
from datetime import datetime


@lru_cache(maxsize=1)  # Will be slow first time, but then caches result
def get_statistics_register() -> dict[str, Any]:
    """Get the overview of all the statistical products from the API.

    Returns:
        dict[str, Any]: The summary of all the products.
    """
    response = rs.get("https://i.ssb.no/statistikkregisteret/statistics")
    response.raise_for_status()
    return json.loads(response.text)["statistics"]


def find_stat_shortcode(
    shortcode_or_id: str = "trosamf",
    get_singles: bool = True,
    get_publishings: bool = True,
    get_publishing_specifics: bool = True,
) -> list[dict[str, Any]]:
    """Find the data for a statistical product by searching by its shortname.

    Args:
        shortcode_or_id (str, optional): The shortname for the statistical product. Defaults to "trosamf".
        get_singles (bool, optional): Get more single data. Defaults to True.
        get_publishings (bool, optional): Get more publishing data. Defaults to True.
        get_publishing_specifics (bool, optional): Get the specific publishings data as well. Defaults to True.

    Returns:
        list[dict[str, Any]]: A data structure containing the found data on the product.
    """
    register = get_statistics_register()
    results = []
    for stat in register:
        # Allow for sending in ID
        if shortcode_or_id.isdigit() and shortcode_or_id in stat["id"]:
            if get_singles:
                stat["product_info"] = single_stat_xml(shortcode_or_id)
            if get_publishings:
                stat["publishings"] = find_publishings(
                    stat["shortName"], get_publishing_specifics
                )
            return stat
        # If not ID, expect to be part of shortname
        elif shortcode_or_id in stat["shortName"]:
            if get_singles:
                stat["product_info"] = single_stat_xml(stat["id"])
            if get_publishings:
                stat["publishings"] = find_publishings(
                    stat["shortName"], get_publishing_specifics
                )
            results += [stat]
    return results


@lru_cache(maxsize=128)
def single_stat_xml(stat_id: str = "4922") -> dict[str, Any]:
    """Get the metadata for specific product.

    Args:
        stat_id (str, optional): The ID for the product in statistikkregisteret. Defaults to "4922".

    Returns:
        dict[str, Any]: Datastructure with the found metadata.
    """
    url = f"https://i.ssb.no/statistikkregisteret/statistikk/xml/{stat_id}"
    result = rs.get(url)
    result.raise_for_status()
    return etree_to_dict(ET.fromstring(result.text))["statistikk"]


@lru_cache(maxsize=128)
def find_publishings(shortname: str = "trosamf",
                     get_publishing_specifics: bool = True) -> dict[str, Any]:
    """Get the publishings for a specific shortcode.

    Args:
        shortname (str, optional): The shortcode to look for in the API among the publishings. Defaults to "trosamf".
        get_publishing_specifics (bool, optional): Looks up more info about each of the publishings found. Defaults to True.

    Returns:
        dict[str, Any]: A datastructure with the found metadata about the publishings.
    """
    url = f"https://i.ssb.no/statistikkregisteret/publisering/listKortnavnSomXml?kortnavn={shortname}"
    result = rs.get(url)
    result.raise_for_status()
    publishings = etree_to_dict(ET.fromstring(result.text))["publiseringer"]
    if get_publishing_specifics:
        for publish in publishings["publisering"]:
            publish["specifics"] = specific_publishing(publish["@id"])
    return publishings


def find_latest_publishing(shortname: str = "trosamf") -> datetime:
    """Find the date of the latest publishing of the statistical product.

    Args:
        shortname (str, optional): The shortname to find the latest publishing for. Defaults to "trosamf".

    Returns:
        datetime: The date the shortcode will have its latest publishing.
    """
    max_date = dateutil.parser.parse("2000-01-01")
    for pub in find_publishings(shortname)["publisering"]:
        current_date = dateutil.parser.parse(
            pub["specifics"]["publisering"]["@tidspunkt"]
        )
        if current_date > max_date:
            max_publ = pub
            max_date = current_date
    return max_publ


@lru_cache(maxsize=128)
def specific_publishing(publish_id: str = "162143") -> dict[str, Any]:
    """Get the publishing-data from a specific publishing-ID in statistikkregisteret.

    Args:
        publish_id (str, optional): The API-ID for the publishing. Defaults to "162143".

    Returns:
        dict[str, Any]: The metadata found for the specific publishing.
    """
    url = f"https://i.ssb.no/statistikkregisteret/publisering/xml/{publish_id}"
    result = rs.get(url)
    result.raise_for_status()
    return etree_to_dict(ET.fromstring(result.text))


def etree_to_dict(t: ET) -> dict[str, Any]:
    """Convert an XML-tree to a python dictionary.

    Args:
        t (ET): The XML element to convert.

    Returns:
        dict[str, Any]: The python dictionary that has been converted to.
    """
    d = {t.tag: {} if t.attrib else None}
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
