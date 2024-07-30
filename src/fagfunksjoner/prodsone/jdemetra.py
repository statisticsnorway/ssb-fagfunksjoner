"""Define time and state for writing XML-files."""

import xml.etree.ElementTree as ET

import pandas as pd
from bs4 import BeautifulSoup

from dapla import FileClient
from fagfunksjoner.fagfunksjoner_logger import logger
from fagfunksjoner.prodsone.check_env import check_env


def df2xml(
    data: pd.DataFrame,
    out: str,
    outpath: str,
    pstart: str,
    ystart: str,
    freq: str
) -> None:
    """Output a XML-file in your specified directory, and logs a message to clarify.

    Parameters:
        data (pd.DataFrame): A Pivoted Pandas DataFrame you want to convert.
        out (str): The name of the outputfile used for JDMETERA+.
        outpath (str): Directory you want to save the output-file.
        pstart (str): The period (month, quarter) you want the sa to start in the first year.
        ystart (str): Startyear of the series.
        freq (str): Yearly frequency of the series. Quarterly is 4, monthly is 12, etc..
    """
    # Deletes the first column in the pivot table (period)
    pivoteddata = data.drop(data.columns[0], axis=1)

    # Start building xml
    root = ET.Element(
        "tsworkspace",
        attrib={
            "xmlns": "eu/tstoolkit:core",
            "xmlns:xsd": "https://www.w3.org/2001/XMLSchema",
            "xmlns:xsi": "https://www.w3.org/2001/XMLSchema-instance",
        },
    )
    timeseries = ET.SubElement(root, "timeseries")
    tscollection = ET.SubElement(timeseries, "tscollection")
    tscollection.set("name", out)
    template = ET.SubElement(tscollection, "data")

    # Looping through colname and series
    for columnName, columnData in pivoteddata.items():
        ts = ET.SubElement(template, "ts")
        navn = columnName
        ts.set("name", navn)
        tsdata = ET.SubElement(
            ts, "tsdata", attrib={"pstart": pstart, "ystart": ystart, "freq": freq}
        )
        data2 = ET.SubElement(tsdata, "data")
        verdier = columnData
        verdier2 = verdier.to_string(index=False)
        verdier3 = verdier2.replace("\n", " ")
        data2.text = verdier3

    xml_data = ET.tostring(root)
    xml_data2 = BeautifulSoup(xml_data, "xml").prettify()

    # If on Dapla
    if check_env() == "DAPLA":
        fs = FileClient.get_gcs_file_system()
        with fs.open(f"{outpath}/{out}.xml", "w") as f:
            f.write(xml_data2)
    else:
        with open(f"{outpath}/{out}.xml", "w") as f:
            f.write(xml_data2)

    logger.info(
        f"""Data has been converted to an XML file ready for JDmetera+ and has been
    been saved at {outpath}/{out}.xml"""
    )
