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
SPACE_LANG = r"@{http://www.w3.org/XML/1998/namespace}lang"


STATUS_MAP = {
    "K": "K: Kommende",
    "A": "A: Aktiv",
    "IA": "IA: Ikke-aktiv",
    "UT": "UT: Opphørt (Utgått/Utgått)",
    "SA": "SA: Sammenslått",
    "SP": "SP: Splittet",
}


@dataclass
class PublishingSpecifics:
    """Hold specific information about each publishing."""

    name: str
    publish_id: str
    statistic: str
    variant: str
    status: str
    is_period: bool
    period_from: datetime.datetime
    period_until: datetime.datetime
    precision: str
    time: datetime.datetime
    has_changed: bool
    desk_flow: str
    time_changed: datetime.datetime
    is_cancelled: bool
    revision: str
    title: str


@dataclass
class StatisticPublishingShort:
    """Top-level metadata for a specific statistical product."""

    stat_id: str
    short_name: str | None
    variant: str
    desk_flow: str
    time_changed: datetime.datetime
    specifics: None | PublishingSpecifics


@dataclass
class MultiplePublishings:
    """Contains multiple statisticss, like when getting all the data in the API."""

    publishings: list[StatisticPublishingShort]
    amount: int
    date: str


@dataclass
class LangText:
    """Represents a text with a language attribute.

    Attributes:
        lang: The language code.
        text: The text in the specified language.
        name: Unused attribute, kept for compatibility.
    """

    lang: str
    text: None | str
    name: None


@dataclass
class Name:
    """Represents a list of LangText objects.

    Attributes:
        name_lang: A list of LangText objects.
    """

    name_lang: list[LangText]


@dataclass
class Contact:
    """Represents a contact with various attributes.

    Attributes:
        name: A list of LangText objects representing names.
        contact_id: The contact ID.
        phone: The contact's phone number.
        cellphone: The contact's cellphonee number.
        email: The contact's email address.
        initials: The contact's initials.
    """

    name: Name
    contact_id: str
    phone: str
    cellphone: str
    email: str
    initials: str
    changed: str | datetime.datetime | None


@dataclass
class Owningsection:
    """Represents an ownership section with various attributes.

    Attributes:
        name: A list of LangText objects representing names.
        section_id: The section ID.
    """

    name: list[LangText]
    section_id: str


@dataclass
class Variant:
    """Represents a variant with various attributes.

    Attributes:
        name: The name of the variant.
        variant_id: The variant ID.
        revision: The revision of the variant.
        ceased: Whether the variant is discontinued.
        detail_level: Detailed level information.
        detail_level_en: Detailed level information in English.
        frequency: The frequency of the variant.
    """

    name: str
    variant_id: str
    revision: str
    ceased: str
    detail_level: str
    detail_level_en: str
    frequency: str


@dataclass
class SinglePublishing:
    """Represents a single publishing entry with various attributes.

    Attributes:
        name: The name details.
        short_name: The short name.
        old_subjectcodes: The old subject codes.
        firstpublishing: The first publication date.
        status: The status code.
        owner_name: The ownership section name.
        owner_code: The ownership section code.
        contacts: A list of contacts.
        triggerwords: A dictionary of trigger words.
        variants: A list of variants.
        regional_levels: A list of regional levels.
        continuation: A dictionary of continuation information.
        publish_id: The ID of the publishing entry.
        default_lang: The default language code.
        approved: Approval status.
        changed: The last modified date.
        desk_flow: Desk flow status.
        dir_flow: Directory flow status.
    """

    name: Name | str
    short_name: str
    old_subjectcodes: str | None
    firstpublishing: str
    status: str
    owningsection: Owningsection
    contacts: list[Contact] | None
    triggerwords: dict[str, list[dict[str, str]]] | None
    variants: list[Variant] | list[str]
    regional_levels: list[str]
    continuation: dict[str, bool] | None
    publish_id: str
    default_lang: str
    approved: str | None
    changed: str | list[str] | None
    desk_flow: str | None
    dir_flow: str | None

    created_date: datetime.datetime | None
    annual_reporting: bool | None
    start_year: str | None
    changes: list[str] | None

    publishings: MultiplePublishings | None


def parse_lang_text_single(entry: dict[str, Any]) -> LangText:
    """Parses a dictionary entry into a LangText object.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        LangText: The parsed LangText object.
    """
    return LangText(
        lang=entry[SPACE_LANG],
        text=entry.get(TEXT, None),
        name=entry.get("@navn", None),
    )


def parse_name_single(entry: dict[str, Any]) -> Name:
    """Parses a dictionary entry into a Name object.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        Name: The parsed Name object.
    """
    return Name(name_lang=[parse_lang_text_single(e) for e in entry["navn"]])


def parse_contact_single(entry: dict[str, Any]) -> Contact:
    """Parses a dictionary entry into a Contact object.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        Contact: The parsed Kontakt object.
    """
    name = Name(name_lang=[parse_lang_text_single(e) for e in entry["navn"]])
    return Contact(
        name=name,
        contact_id=entry["@id"],
        phone=entry["@telefon"],
        cellphone=entry["@mobil"],
        email=entry["@epost"],
        initials=entry["@initialer"],
        changed=entry.get(ENDRET, None),
    )


def parse_eierseksjon_single(entry: dict[str, Any]) -> Owningsection:
    """Parses a dictionary entry into an Owningsection object.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        Owningsection: The parsed Owningsection object.
    """
    name = [parse_lang_text_single(e) for e in entry["navn"]]
    return Owningsection(name=name, section_id=entry["@id"])


def parse_triggerord_single(entry: dict[str, Any]) -> dict[str, str]:
    """Parses a dictionary entry into a trigger word dictionary.

    Args:
        entry: The dictionary entry to parse.

    Returns:
        dict: The parsed trigger word dictionary.
    """
    return {
        "lang": entry[SPACE_LANG],
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
        name=entry["navn"],
        variant_id=entry["@id"],
        revision=entry["@revisjon"],
        ceased=entry["@opphort"],
        detail_level=entry["@detaljniva"],
        detail_level_en=entry["@detaljniva_EN"],
        frequency=entry["@frekvens"],
    )


def parse_data_single(root: dict[str, Any]) -> SinglePublishing:
    """Parses the root dictionary into a SinglePublishing object.

    Args:
        root: The root dictionary to parse.

    Returns:
        SinglePublishing: The parsed SinglePublishing object.
    """
    name = parse_name_single(root["navn"])
    short_name = root["kortnavn"][TEXT]
    old_subjectcodes = root["gamleEmnekoder"]
    firstpublishing = root["forstegangspublisering"]
    try:
        firstpublishing = dateutil.parser.parse(firstpublishing).date()
    except ValueError:
        pass
    status = STATUS_MAP.get(root["status"]["@kode"], root["status"]["@kode"])
    owningsection = parse_eierseksjon_single(root["eierseksjon"])
    contacts = [parse_contact_single(e) for e in root["kontakter"]["kontakt"]]
    triggerwords = {
        k: [parse_triggerord_single(e) for e in v]
        for k, v in root["triggerord"].items()
    }
    # Some times single variants are not in a list already?
    if not isinstance(root["varianter"]["variant"], list):
        root["varianter"]["variant"] = [root["varianter"]["variant"]]
    variants = [
        parse_variant_single(variant) for variant in root["varianter"]["variant"]
    ]
    regional_levels = root["regionaleNivaer"]["kode"]
    continuation = {
        k.replace("@", ""): v == "true" for k, v in root["videreforing"].items()
    }
    publish_id = root["@id"]
    default_lang = root["@defaultLang"]
    approved = root["@godkjent"]
    changed = root[ENDRET]
    try:
        changed = dateutil.parser.parse(changed)
    except ValueError:
        pass
    desk_flow = root[DESKFLYT]
    dir_flow = root["@dirFlyt"]

    return SinglePublishing(
        name=name,
        short_name=short_name,
        old_subjectcodes=old_subjectcodes,
        firstpublishing=firstpublishing,
        status=status,
        owningsection=owningsection,
        contacts=contacts,
        triggerwords=triggerwords,
        variants=variants,
        regional_levels=regional_levels,
        continuation=continuation,
        publish_id=publish_id,
        default_lang=default_lang,
        approved=approved,
        changed=changed,
        desk_flow=desk_flow,
        dir_flow=dir_flow,
        # Missing from this endpoint
        annual_reporting=None,
        start_year=None,
        changes=None,
        created_date=None,
        publishings=None,
    )


def kwargs_specifics(nested: dict[str, Any]) -> dict[str, Any]:
    """Map fields in specifics to kwargs for the dataclass.

    Args:
        nested: The XML-datastructure to map.

    Returns:
        dict[str, Any]: Cleaned up data-structure
    """
    return {
        "name": nested["publisering"]["navn"],
        "publish_id": nested["publisering"]["@id"],
        "statistic": nested["publisering"]["@statistikk"],
        "variant": nested["publisering"]["@variant"],
        "status": STATUS_MAP.get(
            nested["publisering"]["@status"], nested["publisering"]["@status"]
        ),
        "is_period": nested["publisering"]["@erPeriode"] == "true",
        "period_from": dateutil.parser.parse(nested["publisering"]["@periodeFra"]),
        "period_until": dateutil.parser.parse(nested["publisering"]["@periodeTil"]),
        "precision": nested["publisering"]["@presisjon"],
        "time": dateutil.parser.parse(nested["publisering"]["@tidspunkt"]),
        "has_changed": nested["publisering"]["@erEndret"] == "true",
        "desk_flow": nested["publisering"][DESKFLYT],
        "time_changed": dateutil.parser.parse(nested["publisering"][ENDRET]),
        "is_cancelled": nested["publisering"]["@erAvlyst"] == "true",
        "revision": nested["publisering"]["@revisjon"],
        "title": nested["publisering"]["@tittel"],
    }


@lru_cache(maxsize=1)
def get_statistics_register() -> list[dict[str, Any]]:
    """Get the overview of all the statistical products from the API.

    Warning: this may take a little time to fetch.

    Returns:
        dict[str, Any]: The summary of all the products.
    """
    response = rs.get(
        "https://i.ssb.no/statistikkregisteret/statistikk/listAllReleasedAsJson"
    )
    response.raise_for_status()
    stats: list[dict[str, Any]] = response.json()["statistics"]
    return stats


@lru_cache(maxsize=1)
def get_contacts() -> list[Contact]:
    """Get all the contacts from the API.

    Returns:
        list[Contacts]: Each of the contacts in a list.
    """
    response = rs.get("https://i.ssb.no/statistikkregisteret/kontakt/listSomXml")
    response.raise_for_status()
    return parse_contacts(ET.fromstring(response.text))


def parse_contacts(t: ET.Element) -> list[Contact]:
    """Parse the content of contacts into Contact dataclasses.

    Args:
        t: The xml-element to look for contacts in.

    Returns:
        list[Contact]: The parsed data inserted into the dataclasses.
    """
    content = etree_to_dict(t)
    result: list[Contact] = []
    for contact in content["kontakter"]["kontakt"]:
        result.append(
            Contact(
                name=Name(
                    name_lang=[
                        LangText(
                            lang=x[SPACE_LANG],
                            text=x.get("#text", None),
                            name=x.get("@navn", None),
                        )
                        for x in contact["navn"]
                    ]
                ),
                contact_id=contact["@id"],
                phone=contact["@telefon"],
                cellphone=contact["@mobil"],
                email=contact["@epost"],
                initials=contact["@initialer"],
                changed=dateutil.parser.parse(contact[ENDRET]),
            )
        )
    return result


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
        publishings=[
            StatisticPublishingShort(
                stat_id=pub["@id"],
                variant=pub["@variant"],
                desk_flow=pub[DESKFLYT],
                time_changed=dateutil.parser.parse(pub[ENDRET]),
                short_name=pub["@statistikkKortnavn"],
                specifics=pub[
                    "specifics"
                ],  # Already in the correct class from specific_p
            )
            for pub in publishings["publisering"]
        ],
        amount=int(publishings["@antall"]),
        date=publishings["@dato"],
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
        and hasattr(pub.specifics, "time")
    ):

        pub_time: datetime.datetime = pub.specifics.time
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
    for pub in find_publishings(shortname).publishings:
        if (
            isinstance(pub, StatisticPublishingShort)
            and pub.specifics is not None
            and hasattr(pub.specifics, "time")
        ):
            current_date = pub.specifics.time
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


def sections_publishings(
    section_code: str | int,
    include_ceased: bool = False,
    get_publishings: bool = True,
    get_publishing_specifics: bool = True,
) -> list[SinglePublishing]:
    """Get the publishings for a specific owning section at statistics norway.

    Args:
        section_code: The three digit numeric code for the section.
        include_ceased: Set to True if you want to include statistical products that have ceased publiscation. Defaults to False.
        get_publishings: Get more data about all the publishings. Defaults to True.
        get_publishing_specifics: Get more data about every single publishing. Defaults to True.

    Returns:
        list[SinglePublishing]: A list of the sections statistical products.
    """
    register = get_statistics_register()
    section_code_str = str(section_code)
    content: list[SinglePublishing] = [
        parse_single_stat_from_englishjson(stat)
        for stat in register
        if stat["ownerCode"] == section_code_str
    ]
    if not include_ceased:
        content = [
            stat
            for stat in content
            if stat.status not in [STATUS_MAP["IA"], STATUS_MAP["UT"], STATUS_MAP["SA"]]
        ]
    if get_publishings:
        for i, stat in enumerate(content):
            content[i].publishings = find_publishings(
                stat.short_name, get_publishing_specifics
            )
    return content


def parse_single_stat_from_englishjson(stat: dict[str, Any]) -> SinglePublishing:
    """Insert data-parts from the english json endpoint into SinglePublishing object.

    Args:
        stat: The datapart that represents the statistic.

    Returns:
        SinglePublishing: A SinglePublishing object with inserted data.
    """
    return SinglePublishing(
        publish_id=stat["id"],
        short_name=stat["shortName"],
        name=Name(
            name_lang=[
                LangText(name=stat["name"], lang="no", text=None),
                LangText(name=stat["nameEN"], lang="en", text=None),
            ]
        ),
        created_date=dateutil.parser.parse(stat["dateCreated"]),
        default_lang=stat["lang"],
        owningsection=Owningsection(
            name=[LangText(name=stat["owner"], lang="no", text=None)],
            section_id=stat["ownerCode"],
        ),
        status=STATUS_MAP.get(stat["status"], stat["status"]),
        regional_levels=stat["regionalLevels"].split(";"),
        variants=stat["variants"].split(";"),
        annual_reporting=stat["annualReporting"],
        start_year=stat["startYear"],
        firstpublishing=stat["firstReleaseStatistic"],
        changes=stat["changes"],
        # Not available from the english json endpoint
        contacts=None,
        triggerwords=None,
        continuation=None,
        approved=None,
        desk_flow=None,
        dir_flow=None,
        old_subjectcodes=None,
        changed=None,
        publishings=None,
    )


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
