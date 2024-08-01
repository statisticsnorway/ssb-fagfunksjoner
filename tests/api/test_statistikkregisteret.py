import responses
from xml.etree import ElementTree as ET


from fagfunksjoner.api.statistikkregisteret import (
    get_statistics_register,
    find_stat_shortcode,
    single_stat_xml,
    find_publishings,
    find_latest_publishing,
    specific_publishing,
    etree_to_dict,
)

# Helper function to load XML from a string
def load_xml_string(xml_string):
    return ET.ElementTree(ET.fromstring(xml_string))

@responses.activate
def test_get_statistics_register():
    url = "https://i.ssb.no/statistikkregisteret/statistics"
    mock_response = {"statistics": [{"id": "1", "shortName": "test"}]}
    
    responses.add(responses.GET, url, json=mock_response, status=200)
    
    stats = get_statistics_register()
    
    assert isinstance(stats, list)
    assert stats[0]["id"] == "1"
    assert stats[0]["shortName"] == "test"

@responses.activate
def test_find_stat_shortcode():
    url_statistics = "https://i.ssb.no/statistikkregisteret/statistics"
    url_single_stat = "https://i.ssb.no/statistikkregisteret/statistikk/xml/1"
    url_publishings = "https://i.ssb.no/statistikkregisteret/publisering/listKortnavnSomXml?kortnavn=test"
    
    mock_response_statistics = {"statistics": [{"id": "1", "shortName": "test"}]}
    mock_response_single_stat = "<statistikk><id>1</id></statistikk>"
    mock_response_publishings = "<publiseringer><publisering id='1'><tidspunkt>2024-01-01</tidspunkt></publisering></publiseringer>"
    
    responses.add(responses.GET, url_statistics, json=mock_response_statistics, status=200)
    responses.add(responses.GET, url_single_stat, body=mock_response_single_stat, status=200)
    responses.add(responses.GET, url_publishings, body=mock_response_publishings, status=200)
    
    result = find_stat_shortcode("test")
    
    assert isinstance(result, list)
    assert result[0]["id"] == "1"
    assert result[0]["shortName"] == "test"