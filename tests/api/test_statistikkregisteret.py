import datetime

import responses

from fagfunksjoner.api.statistikkregisteret import (
    find_latest_publishing,
    find_publishings,
    find_stat_shortcode,
    get_statistics_register,
    single_stat,
    specific_publishing,
    time_until_publishing,
)


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
    url_specific_publishing = "https://i.ssb.no/statistikkregisteret/publisering/xml/1"

    mock_response_statistics = {"statistics": [{"id": "1", "shortName": "test"}]}
    mock_response_single_stat = """<?xml version="1.0" encoding="UTF-8"?>
<statistikk id='4922' defaultLang='nn' godkjent='true' endret='2024-03-11 09:51:04.003' deskFlyt='GODKJENT' dirFlyt='GODKJENT'>
  <navn>
    <navn xml:lang='no'>Statistikknavn</navn>
    <navn xml:lang='en'>Statistic name</navn>
  </navn>
  <kortnavn id='4921'>test</kortnavn>
  <gamleEmnekoder>07.02.10</gamleEmnekoder>
  <forstegangspublisering>1971</forstegangspublisering>
  <status kode='A' />
  <eierseksjon id='Seksjonssnavn'>
    <navn xml:lang='no' navn='Seksjonen' />
    <navn xml:lang='en' navn='Division' />
  </eierseksjon>
  <kontakter>
    <kontakt id='1' telefon='9999999' mobil='' epost='ola.nordmann@ssb.no' initialer='ola'>
      <navn xml:lang='no'>Ola Nordmann</navn>
      <navn xml:lang='en' />
    </kontakt>
    <kontakt id='2' telefon='9999999' mobil='' epost='kari.nordmann@ssb.no' initialer='kar'>
      <navn xml:lang='no'>Kari Nordmann</navn>
      <navn xml:lang='en' />
    </kontakt>
  </kontakter>
  <triggerord>
    <triggerord xml:lang='no'>medlemmer</triggerord>
    <triggerord xml:lang='en'>members</triggerord>
  </triggerord>
  <varianter>
    <variant id='9803' revisjon='I' opphort='false' detaljniva='' detaljniva_EN='' frekvens='A'>
      <navn>År</navn>
    </variant>
  </varianter>
  <regionaleNivaer>
    <kode>L</kode>
    <kode>F</kode>
  </regionaleNivaer>
  <videreforing harPode='false' harAvlegger='false' />
</statistikk>
"""

    mock_response_publishings = """<?xml version="1.0" encoding="UTF-8"?>
<publiseringer antall='2' dato='Thu Aug 08 11:40:12 CEST 2024'>
  <publisering id='1' variant='9803' deskFlyt='GODKJENT' endret='2011-02-07 16:57:23.626' statistikkKortnavn='test' />
</publiseringer>"""

    mock_response_specific_publishing = """<?xml version="1.0" encoding="UTF-8"?>
<publisering id='162143' statistikk='4922' variant='9803' status='godkjent' erPeriode='false' periodeFra='2020-01-01 00:00:00.0' periodeTil='2020-01-01 00:00:00.0' presisjon='dag' tidspunkt='2020-12-08 08:00:00.0' erEndret='false' deskFlyt='GODKJENT' endret='2020-08-31 13:57:13.089' erAvlyst='false' revisjon='I' tittel='test 2020-12-08'>
  <navn>01.01 2020</navn>
</publisering>"""

    responses.add(
        responses.GET, url_statistics, json=mock_response_statistics, status=200
    )
    responses.add(
        responses.GET, url_single_stat, body=mock_response_single_stat, status=200
    )
    responses.add(
        responses.GET, url_publishings, body=mock_response_publishings, status=200
    )
    responses.add(
        responses.GET,
        url_specific_publishing,
        body=mock_response_specific_publishing,
        status=200,
    )

    result = find_stat_shortcode("test")

    assert isinstance(result, list)
    assert result[0]["id"] == "1"
    assert result[0]["shortName"] == "test"
    assert hasattr(result[0]["publishings"].publiseringer[0], "specifics")
    assert result[0]["publishings"].publiseringer[0].specifics.statid == "162143"

    assert isinstance(time_until_publishing("test"), datetime.timedelta)
    assert single_stat("1").triggerord["triggerord"][0]["lang"] == "no"
    assert isinstance(find_latest_publishing("test").endret, datetime.datetime)
    assert find_publishings("test").publiseringer[0].statid.isdigit()
    assert isinstance(specific_publishing("1").er_avlyst, bool)
