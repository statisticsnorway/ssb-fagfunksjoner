import datetime
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from xml.etree import ElementTree as ET

import dateutil.parser
import requests as rs

from fagfunksjoner.fagfunksjoner_logger import logger


@dataclass
class PublishingSpecifics:
    """Hold specific information about each publishing."""

    navn: str
    statid: str
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
class StatisticPublishingShort:
    """Top-level metadata for a specific statistical product."""

    statid: str
    variant: str
    deskFlyt: str
    endret: datetime.datetime
    statistikkKortnavn: str
    specifics: None | PublishingSpecifics


@dataclass
class MultiplePublishings:
    """Contains multiple statisticss, like when getting all the data in the API."""

    publiseringer: list[StatisticPublishingShort]
    antall: int
    dato: str


@dataclass
class LangText:
    """Represents a text with a language attribute.

    Attributes:
        lang (str): The language code.
        text (None | str): The text in the specified language.
        navn (None): Unused attribute, kept for compatibility.
    """

    lang: str
    text: None | str
    navn: None


@dataclass
class Navn:
    """Represents a list of LangText objects.

    Attributes:
        navn (list[LangText]): A list of LangText objects.
    """

    navn: list[LangText]


@dataclass
class Kontakt:
    """Represents a contact with various attributes.

    Attributes:
        navn (list[LangText]): A list of LangText objects representing names.
        statid (str): The contact ID.
        telefon (str): The contact's phone number.
        mobil (str): The contact's mobile number.
        epost (str): The contact's email address.
        initialer (str): The contact's initials.
    """

    navn: list[LangText]
    statid: str
    telefon: str
    mobil: str
    epost: str
    initialer: str


@dataclass
class Eierseksjon:
    """Represents an ownership section with various attributes.

    Attributes:
        navn (list[LangText]): A list of LangText objects representing names.
        statid (str): The section ID.
        navn_attr (str): The section name.
    """

    navn: list[LangText]
    statid: str
    navn_attr: str


@dataclass
class Variant:
    """Represents a variant with various attributes.

    Attributes:
        navn (str): The name of the variant.
        statid (str): The variant ID.
        revisjon (str): The revision of the variant.
        opphort (str): Whether the variant is discontinued.
        detaljniva (str): Detailed level information.
        detaljniva_EN (str): Detailed level information in English.
        frekvens (str): The frequency of the variant.
    """

    navn: str
    statid: str
    revisjon: str
    opphort: str
    detaljniva: str
    detaljniva_EN: str
    frekvens: str


@dataclass
class SinglePublishing:
    """Represents a single publishing entry with various attributes.

    Attributes:
        navn (Navn): The name details.
        kortnavn (str): The short name.
        gamleEmnekoder (str): The old subject codes.
        forstegangspublisering (str): The first publication date.
        status (str): The status code.
        eierseksjon (Eierseksjon): The ownership section details.
        kontakter (list[Kontakt]): A list of contacts.
        triggerord (dict[str, list[dict[str, str]]]): A dictionary of trigger words.
        varianter (list[Variant]): A list of variants.
        regionaleNivaer (list[str]): A list of regional levels.
        videreforing (dict): A dictionary of continuation information.
        statid (str): The ID of the publishing entry.
        defaultLang (str): The default language code.
        godkjent (str): Approval status.
        endret (str): The last modified date.
        deskFlyt (str): Desk flow status.
        dirFlyt (str): Directory flow status.
    """

    navn: Navn
    kortnavn: str
    gamleEmnekoder: str
    forstegangspublisering: str
    status: str
    eierseksjon: Eierseksjon
    kontakter: list[Kontakt]
    triggerord: dict[str, list[dict[str, str]]]
    varianter: list[Variant]
    regionaleNivaer: list[str]
    videreforing: dict
    statid: str
    defaultLang: str
    godkjent: str
    endret: str
    deskFlyt: str
    dirFlyt: str


def parse_lang_text_single(entry: dict[str, Any]) -> LangText:
    """Parses a dictionary entry into a LangText object.

    Args:
        entry (dict[str, Any]): The dictionary entry to parse.

    Returns:
        LangText: The parsed LangText object.
    """
    return LangText(
        lang=entry["@{http://www.w3.org/XML/1998/namespace}lang"],
        text=entry.get("#text", None),
        navn=entry.get("@navn", None),
    )


def parse_navn_single(entry: dict[str, Any]) -> Navn:
    """Parses a dictionary entry into a Navn object.

    Args:
        entry (dict[str, Any]): The dictionary entry to parse.

    Returns:
        Navn: The parsed Navn object.
    """
    return Navn(navn=[parse_lang_text_single(e) for e in entry["navn"]])


def parse_kontakt_single(entry: dict[str, Any]) -> Kontakt:
    """Parses a dictionary entry into a Kontakt object.

    Args:
        entry (dict[str, Any]): The dictionary entry to parse.

    Returns:
        Kontakt: The parsed Kontakt object.
    """
    navn = [parse_lang_text_single(e) for e in entry["navn"]]
    return Kontakt(
        navn=navn,
        statid=entry["@id"],
        telefon=entry["@telefon"],
        mobil=entry["@mobil"],
        epost=entry["@epost"],
        initialer=entry["@initialer"],
    )


def parse_eierseksjon_single(entry: dict[str, Any]) -> Eierseksjon:
    """Parses a dictionary entry into an Eierseksjon object.

    Args:
        entry (dict[str, Any]): The dictionary entry to parse.

    Returns:
        Eierseksjon: The parsed Eierseksjon object.
    """
    navn = [parse_lang_text_single(e) for e in entry["navn"]]
    return Eierseksjon(navn=navn, statid=entry["@id"], navn_attr=entry["@navn"])


def parse_triggerord_single(entry: dict[str, Any]) -> dict[str, str]:
    """Parses a dictionary entry into a trigger word dictionary.

    Args:
        entry (dict[str, Any]): The dictionary entry to parse.

    Returns:
        dict: The parsed trigger word dictionary.
    """
    return {
        "lang": entry["@{http://www.w3.org/XML/1998/namespace}lang"],
        "text": entry["#text"],
    }


def parse_variant_single(entry: dict[str, Any]) -> Variant:
    """Parses a dictionary entry into a Variant object.

    Args:
        entry (dict[str, Any]): The dictionary entry to parse.

    Returns:
        Variant: The parsed Variant object.
    """
    logger.info(f"{entry}")
    return Variant(
        navn=entry["navn"],
        statid=entry["@id"],
        revisjon=entry["@revisjon"],
        opphort=entry["@opphort"],
        detaljniva=entry["@detaljniva"],
        detaljniva_EN=entry["@detaljniva_EN"],
        frekvens=entry["@frekvens"],
    )


def parse_data_single(root: dict[str, Any]) -> SinglePublishing:
    """Parses the root dictionary into a SinglePublishing object.

    Args:
        root (dict[str, Any]): The root dictionary to parse.

    Returns:
        SinglePublishing: The parsed SinglePublishing object.
    """
    navn = parse_navn_single(root["navn"])
    kortnavn = root["kortnavn"]["#text"]
    gamleEmnekoder = root["gamleEmnekoder"]
    forstegangspublisering = root["forstegangspublisering"]
    try:
        forstegangspublisering = dateutil.parser.parse(forstegangspublisering).date()
    except ValueError:
        pass
    status = root["status"]["@kode"]
    eierseksjon = parse_eierseksjon_single(root["eierseksjon"])
    kontakter = [parse_kontakt_single(e) for e in root["kontakter"]["kontakt"]]
    triggerord = [parse_triggerord_single(e) for e in root["triggerord"]["triggerord"]]
    # Some times single variants are not in a list already?
    if not isinstance(root["varianter"]["variant"], list):
        root["varianter"]["variant"] = [root["varianter"]["variant"]]
    varianter = [
        parse_variant_single(variant) for variant in root["varianter"]["variant"]
    ]
    regionaleNivaer = root["regionaleNivaer"]["kode"]
    videreforing = root["videreforing"]
    statid = root["@id"]
    defaultLang = root["@defaultLang"]
    godkjent = root["@godkjent"]
    endret = root["@endret"]
    try:
        endret = dateutil.parser.parse(endret)
    except ValueError:
        pass
    deskFlyt = root["@deskFlyt"]
    dirFlyt = root["@dirFlyt"]

    return SinglePublishing(
        navn=navn,
        kortnavn=kortnavn,
        gamleEmnekoder=gamleEmnekoder,
        forstegangspublisering=forstegangspublisering,
        status=status,
        eierseksjon=eierseksjon,
        kontakter=kontakter,
        triggerord=triggerord,
        varianter=varianter,
        regionaleNivaer=regionaleNivaer,
        videreforing=videreforing,
        statid=statid,
        defaultLang=defaultLang,
        godkjent=godkjent,
        endret=endret,
        deskFlyt=deskFlyt,
        dirFlyt=dirFlyt,
    )


def kwargs_specifics(nested: dict[str, Any]) -> dict[str, Any]:
    """Map fields in specifics to kwargs for the dataclass.

    Args:
        nested (dict[str, Any]): The XML-datastructure to map.

    Returns:
        dict[str, Any]: Cleaned up data-structure
    """
    return {
        "navn": nested["publisering"]["navn"],
        "statid": nested["publisering"]["@id"],
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
                stat["product_info"] = single_stat(shortcode_or_id)
            if get_publishings:
                stat["publishings"] = find_publishings(
                    stat["shortName"], get_publishing_specifics
                )
            return stat
        elif shortcode_or_id in stat["shortName"]:
            if get_singles:
                stat["product_info"] = single_stat(stat["id"])
            if get_publishings:
                stat["publishings"] = find_publishings(
                    stat["shortName"], get_publishing_specifics
                )
            results.append(stat)
    return results


@lru_cache(maxsize=128)
def single_stat(stat_id: str = "4922") -> SinglePublishing:
    """Get the metadata for specific product.

    Args:
        stat_id (str): The ID for the product in statistikkregisteret. Defaults to "4922".

    Returns:
        SinglePublishing: Datastructure with the found metadata.
    """
    url = f"https://i.ssb.no/statistikkregisteret/statistikk/xml/{stat_id}"
    result = rs.get(url)
    result.raise_for_status()
    nested: dict[str, Any] = etree_to_dict(ET.fromstring(result.text))["statistikk"]
    return parse_data_single(nested)


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
        publiseringer=[
            StatisticPublishingShort(
                statid=pub["@id"],
                variant=pub["@variant"],
                deskFlyt=pub["@deskFlyt"],
                endret=dateutil.parser.parse(pub["@endret"]),
                statistikkKortnavn=pub["@statistikkKortnavn"],
                specifics=pub[
                    "specifics"
                ],  # Already in the correct class from specific_p
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
    if (isinstance(pub, StatisticPublishingShort) 
        and pub.specifics is not None 
        and hasattr(pub.specifics, "tidspunkt")):
        
        pub_time: datetime.datetime = pub.specifics.tidspunkt
        diff_time: datetime.timedelta = pub_time - datetime.datetime.now()
        return diff_time
    return None


def find_latest_publishing(
    shortname: str = "trosamf",
) -> StatisticPublishingShort | None:
    """Find the date of the latest publishing of the statistical product.

    Args:
        shortname (str): The shortname to find the latest publishing for. Defaults to "trosamf".

    Returns:
        StatisticPublishingShort | None: data about the specific publishing. Or None if nothing is found.
    """
    max_date = dateutil.parser.parse("2000-01-01")
    max_publ: StatisticPublishingShort | None = None
    # Loop over publishings to find the one with the highest date (latest)
    for pub in find_publishings(shortname).publiseringer:
        if (isinstance(pub, StatisticPublishingShort) 
            and pub.specifics is not None 
            and hasattr(pub.specifics, "tidspunkt")):
            current_date = pub.specifics.tidspunkt
            if current_date > max_date:
                max_publ = pub  # Overwrites variable with the whole StatisticPublishingShort
                max_date = current_date
    return max_publ


@lru_cache(maxsize=128)
def specific_publishing(publish_id: str = "162143") -> PublishingSpecifics:
    """Get the publishing-data from a specific publishing-ID in statistikkregisteret.

    Args:
        publish_id (str): The API-ID for the publishing. Defaults to "162143".

    Returns:
        PublishingSpecifics: The metadata found for the specific publishing.
    """
    url = f"https://i.ssb.no/statistikkregisteret/publisering/xml/{publish_id}"
    result = rs.get(url)
    result.raise_for_status()
    nested: dict[str, Any] = etree_to_dict(ET.fromstring(result.text))
    return PublishingSpecifics(**kwargs_specifics(nested))


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
