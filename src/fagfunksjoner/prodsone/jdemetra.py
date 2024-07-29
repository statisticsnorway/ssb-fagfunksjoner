import fnmatch
import os
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

from fagfunksjoner.fagfunksjoner_logger import logger

def df2xml(data, out: str, outpath: str, pstart: str, ystart: str, freq: str):
    """
    Parameters:
        data : A Pandas DataFrame you want to convert.
        out : The name of the outputfile and attribute name in tscollection.
        outpath : Directory you want to save the output-file.
        pstart : The period (month, quarter) you want the sa to start in the first year.
        ystart : Startyear of the series.
        freq : Yearly frequency of the series. Quarterly is 4, monthly is 12, etc..
    Returns:
        An XML-file in your specified directory.
    """
    # Deletes the first column since its because in our data thats a period-column
    RawSeries2 = data.drop(data.columns[0], axis=1)
    # Start building xml
    root = ET.Element(
        "tsworkspace",
        attrib={
            "xmlns": "eu/tstoolkit:core",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        },
    )
    timeseries = ET.SubElement(root, "timeseries")
    tscollection = ET.SubElement(timeseries, "tscollection")
    tscollection.set("name", out)
    data = ET.SubElement(tscollection, "data")

    # Looping through colname and series
    for columnName, columnData in RawSeries2.iteritems():
        ts = ET.SubElement(data, "ts")
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
    with open(f"{outpath}/{out}.xml", "w") as f:
        f.write(xml_data2)

    logger.info(
        f"""Pandas DataFrame has been converted to an XML and has
    been saved at {outpath}/{out}.xml"""
    )


def replace_input_paths(directory, find, replace, filePattern):
    """
    Parameters:
        directory: Directory where you want (recursively) search through xml- and bak-files.
        find: The text-string you want to search replace. Use the full path, i.e. "/ssb/stamme01/vakanse/wk1"
        replace: The text-string you want to insert. Use full path, i.e. "/home/jovyan/repos/sesjust/"
        filePattern: List of filepatterns to search through. For example: ["*.xml", "*.bak"])

    Returns:
        Modifys in place, aka returns the same, but modified, files.

    """
    find = find.replace("/", "%2F")
    replace = replace.replace("/", "%2F")
    for path, dirs, files in os.walk(os.path.abspath(directory)):
        for filending in filePattern:
            for filename in fnmatch.filter(files, filending):
                filepath = os.path.join(path, filename)
                with open(filepath) as f:
                    s = f.read()
                s = s.replace(find, replace)
                with open(filepath, "w") as f:
                    f.write(s)
