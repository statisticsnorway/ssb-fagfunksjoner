import requests as rs
import json
from functools import lru_cache
from collections import defaultdict
from xml.etree import cElementTree as ET
import dateutil
import datetime


@lru_cache(maxsize=1)  # Will be slow first time, but then caches result
def get_statistics_register() -> dict:
    response = rs.get("https://i.ssb.no/statistikkregisteret/statistics")
    return json.loads(response.text)['statistics']


def find_stat_shortcode(shortcode_or_id: str = "trosamf", 
                        get_singles: bool = True,
                        get_publishings: bool = True,
                        get_publishing_specifics: bool = True) -> list:
    register = get_statistics_register()
    results = []
    for stat in register:
        # Allow for sending in ID
        if shortcode_or_id.isdigit() and shortcode_or_id in stat["id"]:
            if get_singles: 
                stat["product_info"] = single_stat_xml(shortcode_or_id)
            if get_publishings: 
                stat["publishings"] = find_publishings(stat["shortName"], get_publishing_specifics)
            return stat
        # If not ID, expect to be part of shortname
        elif shortcode_or_id in stat["shortName"]:
            if get_singles: 
                stat["product_info"] = single_stat_xml(stat['id'])
            if get_publishings:
                stat["publishings"] = find_publishings(stat["shortName"], get_publishing_specifics)
            results += [stat]
    return results


@lru_cache(maxsize=128)
def single_stat_xml(stat_id: str = "4922"):
    url = f"https://i.ssb.no/statistikkregisteret/statistikk/xml/{stat_id}"
    return etree_to_dict(ET.fromstring(rs.get(url).text))["statistikk"]


@lru_cache(maxsize=128)
def find_publishings(shortname: str = "trosamf", get_publishing_specifics: bool = True):
    url = f"https://i.ssb.no/statistikkregisteret/publisering/listKortnavnSomXml?kortnavn={shortname}"
    publishings = etree_to_dict(ET.fromstring(rs.get(url).text))["publiseringer"]
    if get_publishing_specifics:
        for publish in publishings["publisering"]:
            publish["specifics"] = specific_publishing(publish["@id"])
    return publishings

def find_latest_publishing(shortname: str = "trosamf"):
    max_date = dateutil.parser.parse("2000-01-01")
    for pub in find_publishings(shortname)["publisering"]:
        current_date = dateutil.parser.parse(pub['specifics']['publisering']["@tidspunkt"])
        if current_date > max_date:
            max_publ = pub
            max_date = current_date
    return max_publ


def time_until_publishing(kortkode: str) -> datetime.timedelta:
    tid = find_latest_publishing(kortkode)["specifics"]["publisering"]['@tidspunkt']
    tid = datetime.datetime.strptime(tid[:-2], "%Y-%m-%d %H:%M:%S")
    return tid - datetime.datetime.now()


@lru_cache(maxsize=128)
def specific_publishing(publish_id: str = "162143"):
    url = f"https://i.ssb.no/statistikkregisteret/publisering/xml/{publish_id}"
    return etree_to_dict(ET.fromstring(rs.get(url).text))


def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d