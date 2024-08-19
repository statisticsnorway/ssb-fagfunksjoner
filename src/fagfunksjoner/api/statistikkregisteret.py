import datetime
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from xml.etree import ElementTree as ET

import dateutil.parser
import requests as rs

from fagfunksjoner.fagfunksjoner_logger import logger


TEXT = "#text"
ENDRET = "@endret"
DESKFLYT = "@deskFlyt"


@dataclass
class PublishingSpecifics:
    """Hold specific information about each publishing."""

    navn: str
    statid: str
    statistikk: str
    variant: str
    status: str
    er_periode: bool
    periode_fra: datetime.datetime
    periode_til: datetime.datetime
    presisjon: str
    tidspunkt: datetime.datetime
    er_endret: bool
    desk_flyt: str
    endret: datetime.datetime
    er_avlyst: bool
    revisjon: str
    tittel: str


@dataclass
class StatisticPublishingShort:
    """Top-level metadata for a specific statistical product."""

    statid: str
    variant: str
    desk_flyt: str
    endret: datetime.datetime
    statistikk_kortnavn: str
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
        lang: The language code.
        text: The text in the specified language.
        navn: Unused attribute, kept for compatibility.
    """

    lang: str
    text: None | str
    navn: None


@dataclass
class Navn:
    """Represents a list of LangText objects.

    Attributes:
        navn_lang: A list of LangText objects.
    """

    navn_lang: list[LangText]


@dataclass
class Kontakt:
    """Represents a contact with various attributes.

    Attributes:
        navn: A list of LangText objects representing names.
        statid: The contact ID.
        telefon: The contact's phone number.
        mobil: The contact's mobile number.
        epost: The contact's email address.
        initialer: The contact's initials.
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
        navn: A list of LangText objects representing names.
        statid: The section ID.
        navn_attr: The section name.
    """

    navn: list[LangText]
    statid: str


@dataclass
class Variant:
    """Represents a variant with various attributes.

    Attributes:
        navn: The name of the variant.
        statid: The variant ID.
        revisjon: The revision of the variant.
        opphort: Whether the variant is discontinued.
        detaljniva: Detailed level information.
        detaljniva_EN: Detailed level information in English.
        frekvens: The frequency of the variant.
    """

    navn: str
    statid: str
    revisjon: str
    opphort: str
    detaljniva: str
    detaljniva_en: str
    frekvens: str


@dataclass
class SinglePublishing:
    """Represents a single publishing entry with various attributes.

    Attributes:
        navn: The name details.
        kortnavn: The short name.
        gamleEmnekoder: The old subject codes.
        forstegangspublisering: The first publication date.
        status: The status code.
        eierseksjon: The ownership section details.
        kontakter: A list of contacts.
        triggerord: A dictionary of trigger words.
        varianter: A list of variants.
        regionaleNivaer: A list of regional levels.
        videreforing: A dictionary of continuation information.
        statid: The ID of the publishing entry.
        defaultLang: The default language code.
        godkjent: Approval status.
        endret: The last modified date.
        deskFlyt: Desk flow status.
        dirFlyt: Directory flow status.
    """

    navn: Navn
    kortnavn: str
    gamle_emnekoder: str
    forstegangspublisering: str
    status: str
    eierseksjon: Eierseksjon
    kontakter: list[Kontakt]
    triggerord: dict[str, list[dict[str, str]]]
    varianter: list[Variant]
    regionale_nivaer: list[str]
    videreforing: dict[str, bool]
    statid: str
    default_lang: str
    godkjent: str
    endret: str
    desk_flyt: str
    dir_flyt: str


def parse_lang_text_single(entry: dict[str, Any]) -> LangText:
    """Parses a dictionary entry into a LangText object.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        LangText: The parsed LangText object.
    """
    return LangText(
        lang=entry["@{http://www.w3.org/XML/1998/namespace}lang"],
        text=entry.get(TEXT, None),
        navn=entry.get("@navn", None),
    )


def parse_navn_single(entry: dict[str, Any]) -> Navn:
    """Parses a dictionary entry into a Navn object.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        Navn: The parsed Navn object.
    """
    return Navn(navn_lang=[parse_lang_text_single(e) for e in entry["navn"]])


def parse_kontakt_single(entry: dict[str, Any]) -> Kontakt:
    """Parses a dictionary entry into a Kontakt object.

    Args:
        entry: The dictionary entry to parse.

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
        entry: The dictionary entry to parse.

    Returns:
        Eierseksjon: The parsed Eierseksjon object.
    """
    navn = [parse_lang_text_single(e) for e in entry["navn"]]
    return Eierseksjon(navn=navn, statid=entry["@id"])


def parse_triggerord_single(entry: dict[str, Any]) -> dict[str, str]:
    """Parses a dictionary entry into a trigger word dictionary.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        dict: The parsed trigger word dictionary.
    """
    return {
        "lang": entry["@{http://www.w3.org/XML/1998/namespace}lang"],
        "text": entry[TEXT],
    }


def parse_variant_single(entry: dict[str, Any]) -> Variant:
    """Parses a dictionary entry into a Variant object.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        Variant: The parsed Variant object.
    """
    return Variant(
        navn=entry["navn"],
        statid=entry["@id"],
        revisjon=entry["@revisjon"],
        opphort=entry["@opphort"],
        detaljniva=entry["@detaljniva"],
        detaljniva_en=entry["@detaljniva_EN"],
        frekvens=entry["@frekvens"],
    )


def parse_data_single(root: dict[str, Any]) -> SinglePublishing:
    """Parses the root dictionary into a SinglePublishing object.

    Args:
        root: The root dictionary to parse.

    Returns:
        SinglePublishing: The parsed SinglePublishing object.
    """
    navn = parse_navn_single(root["navn"])
    kortnavn = root["kortnavn"][TEXT]
    gamle_emnekoder = root["gamleEmnekoder"]
    forstegangspublisering = root["forstegangspublisering"]
    try:
        forstegangspublisering = dateutil.parser.parse(forstegangspublisering).date()
    except ValueError:
        pass
    status = root["status"]["@kode"]
    eierseksjon = parse_eierseksjon_single(root["eierseksjon"])
    kontakter = [parse_kontakt_single(e) for e in root["kontakter"]["kontakt"]]
    triggerord = {
        k: [parse_triggerord_single(e) for e in v]
        for k, v in root["triggerord"].items()
    }
    # Some times single variants are not in a list already?
    if not isinstance(root["varianter"]["variant"], list):
        root["varianter"]["variant"] = [root["varianter"]["variant"]]
    varianter = [
        parse_variant_single(variant) for variant in root["varianter"]["variant"]
    ]
    regionale_nivaer = root["regionaleNivaer"]["kode"]
    videreforing = {
        k.replace("@", ""): v == "true" for k, v in root["videreforing"].items()
    }
    statid = root["@id"]
    default_lang = root["@defaultLang"]
    godkjent = root["@godkjent"]
    endret = root[ENDRET]
    try:
        endret = dateutil.parser.parse(endret)
    except ValueError:
        pass
    desk_flyt = root[DESKFLYT]
    dir_flyt = root["@dirFlyt"]

    return SinglePublishing(
        navn=navn,
        kortnavn=kortnavn,
        gamle_emnekoder=gamle_emnekoder,
        forstegangspublisering=forstegangspublisering,
        status=status,
        eierseksjon=eierseksjon,
        kontakter=kontakter,
        triggerord=triggerord,
        varianter=varianter,
        regionale_nivaer=regionale_nivaer,
        videreforing=videreforing,
        statid=statid,
        default_lang=default_lang,
        godkjent=godkjent,
        endret=endret,
        desk_flyt=desk_flyt,
        dir_flyt=dir_flyt,
    )


def kwargs_specifics(nested: dict[str, Any]) -> dict[str, Any]:
    """Map fields in specifics to kwargs for the dataclass.

    Args:
        nested: The XML-datastructure to map.

    Returns:
        dict[str, Any]: Cleaned up data-structure
    """
    return {
        "navn": nested["publisering"]["navn"],
        "statid": nested["publisering"]["@id"],
        "statistikk": nested["publisering"]["@statistikk"],
        "variant": nested["publisering"]["@variant"],
        "status": nested["publisering"]["@status"],
        "er_periode": nested["publisering"]["@erPeriode"] == "true",
        "periode_fra": dateutil.parser.parse(nested["publisering"]["@periodeFra"]),
        "periode_til": dateutil.parser.parse(nested["publisering"]["@periodeTil"]),
        "presisjon": nested["publisering"]["@presisjon"],
        "tidspunkt": dateutil.parser.parse(nested["publisering"]["@tidspunkt"]),
        "er_endret": nested["publisering"]["@erEndret"] == "true",
        "desk_flyt": nested["publisering"][DESKFLYT],
        "endret": dateutil.parser.parse(nested["publisering"][ENDRET]),
        "er_avlyst": nested["publisering"]["@erAvlyst"] == "true",
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
        shortcode_or_id: The shortname for the statistical product. Defaults to "trosamf".
        get_singles: Get more single data. Defaults to True.
        get_publishings: Get more publishing data. Defaults to True.
        get_publishing_specifics: Get the specific publishings data as well. Defaults to True.

    Returns:
        list[dict[str, Any]]: A data structure containing the found data on the product.
    """
    register = get_statistics_register()
    results = []
    for stat in register:
        if shortcode_or_id.isdigit() and shortcode_or_id == stat["id"]:
            return get_singles_publishings(
                stat,
                shortcode_or_id,
                get_singles,
                get_publishings,
                get_publishing_specifics,
            )
        elif shortcode_or_id in stat["shortName"]:
            results.append(
                get_singles_publishings(
                    stat,
                    stat["id"],
                    get_singles,
                    get_publishings,
                    get_publishing_specifics,
                )
            )
    return results


def get_singles_publishings(
    stat: dict[str, Any],
    shortcode_or_id: str = "trosamf",
    get_singles: bool = True,
    get_publishings: bool = True,
    get_publishing_specifics: bool = True,
) -> dict[str, Any]:
    """Find the data for a statistical product by searching by its shortname.

    Args:
        stat: A single stat from the register.
        shortcode_or_id: The shortname for the statistical product. Defaults to "trosamf".
        get_singles: Get more single data. Defaults to True.
        get_publishings: Get more publishing data. Defaults to True.
        get_publishing_specifics: Get the specific publishings data as well. Defaults to True.

    Returns:
        list[dict[str, Any]]: A data structure containing the found data on the product.
    """
    if get_singles:
        stat["product_info"] = single_stat(shortcode_or_id)
    if get_publishings:
        stat["publishings"] = find_publishings(
            stat["shortName"], get_publishing_specifics
        )
    return stat


@lru_cache(maxsize=128)
def single_stat(stat_id: str = "4922") -> SinglePublishing:
    """Get the metadata for specific product.

    Args:
        stat_id: The ID for the product in statistikkregisteret. Defaults to "4922".

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
        shortname: The shortcode to look for in the API among the publishings. Defaults to "trosamf".
        get_publishing_specifics: Looks up more info about each of the publishings found. Defaults to True.

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
                desk_flyt=pub[DESKFLYT],
                endret=dateutil.parser.parse(pub[ENDRET]),
                statistikk_kortnavn=pub["@statistikkKortnavn"],
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
        shortname: The shortcode to look for in the API among the publishings. Defaults to "trosamf".

    Returns:
        datetime.timedelta | None : The time difference between now, and the latest publishing date.
            If no publishingdata is found, returns None.
    """
    pub = find_latest_publishing(shortname)
    if (
        isinstance(pub, StatisticPublishingShort)
        and pub.specifics is not None
        and hasattr(pub.specifics, "tidspunkt")
    ):

        pub_time: datetime.datetime = pub.specifics.tidspunkt
        diff_time: datetime.timedelta = pub_time - datetime.datetime.now()
        return diff_time
    return None


def find_latest_publishing(
    shortname: str = "trosamf",
) -> StatisticPublishingShort | None:
    """Find the date of the latest publishing of the statistical product.

    Args:
        shortname: The shortname to find the latest publishing for. Defaults to "trosamf".

    Returns:
        StatisticPublishingShort | None: data about the specific publishing. Or None if nothing is found.
    """
    max_date = dateutil.parser.parse("2000-01-01")
    max_publ: StatisticPublishingShort | None = None
    # Loop over publishings to find the one with the highest date (latest)
    for pub in find_publishings(shortname).publiseringer:
        if (
            isinstance(pub, StatisticPublishingShort)
            and pub.specifics is not None
            and hasattr(pub.specifics, "tidspunkt")
        ):
            current_date = pub.specifics.tidspunkt
            if current_date > max_date:
                max_publ = (
                    pub  # Overwrites variable with the whole StatisticPublishingShort
                )
                max_date = current_date
    return max_publ


@lru_cache(maxsize=128)
def specific_publishing(publish_id: str = "162143") -> PublishingSpecifics:
    """Get the publishing-data from a specific publishing-ID in statistikkregisteret.

    Args:
        publish_id: The API-ID for the publishing. Defaults to "162143".

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
        t: The XML element to convert.

    Returns:
        dict[str, Any]: The python dictionary that has been converted to.
    """
    d: dict[str, Any] = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        d = handle_children(children, t)
    if t.attrib:
        d[t.tag].update(("@" + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag][TEXT] = text
        else:
            d[t.tag] = text
    return d


def handle_children(children: list[ET.Element], t: ET.Element) -> dict[str, Any]:
    """Handle children in the etree.

    Args:
        children: The children to treat.
        t: The XML element to convert.

    Returns:
        dict[str, Any]: The python dictionary of the children part.
    """
    dd = defaultdict(list)
    for dc in map(etree_to_dict, children):
        for k, v in dc.items():
            dd[k].append(v)
    return {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}


def raise_on_missing_future_publish(
    shortname: str, raise_error: bool = True
) -> datetime.timedelta | None:
    """Check the next time the product should be published. Possibly raising an error.

    Args:
        shortname: The shortname for the statistical product to look for.
        raise_error: Set to False, if you just want to log that statistical product has no future publishings.

    Returns:
        datetime.timedelta: Time until next publishing.
    """
    zerotime = datetime.timedelta(0)
    next_time = time_until_publishing(shortname)
    if next_time is None:
        msg = f"Cant find any publishing times for {shortname}. Are you using the correct shortname?"
        _raise_publisherror_possibly(raise_error, msg)
        return next_time

    elif isinstance(next_time, datetime.timedelta) and next_time < zerotime:
        msg = f"You haven't added a next publishing to the register yet? {next_time.days} days since last publishing?"
        _raise_publisherror_possibly(raise_error, msg)
    logger.info(f"Publishing in {next_time.days} days, according to register.")
    return next_time


class FuturePublishingError(Exception):
    """Missing expected publishing at a future time."""

    ...


def _raise_publisherror_possibly(raise_error: bool, msg: str) -> None:
    """Raise an error on missing future publish, or log, depending on bool flag raise_error.

    Args:
        raise_error: True if raising error, logs a warning otherwise.
        msg: The message to raise the error with, or to log.

    Raises:
        FuturePublishingError: If raise_error i set to True, raises an error if there are no future publishings.

    Returns:
        None: Function only has side-effects.
    """
    if raise_error:
        raise FuturePublishingError(msg)
    logger.warning(msg)
    return None
