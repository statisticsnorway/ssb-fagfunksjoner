import xml.etree.ElementTree as ET

from fagfunksjoner.fagfunksjoner_logger import logger


def change_input_to_xml(
    spec_path: str,
    spec_filename: str,
    input_path: str,
    input_filename: str,
    output_path: str,
) -> None:
    """Parse specification-files from Jdemetra+ and replaces references to old file format and replaces them with XML-file-references.

    Args:
        spec_path (str): Path to where your specification-files are localed. Typically in a folder called SAProcessing/
        spec_filename (str): Name of specification-file. Default from Jdemetra+ is SAProcessing-1, SAProcessing-2, etc.
        input_path (str): Path to directory where input-files are located. I.e. ~/sa/project/input
        input_filename (str): Name of input-file for the specfication stated in the first two arguments.
        output_path (str): Path to where you want to save the new specification-files.

    """
    path = spec_path
    tree = ET.parse(path + "/" + spec_filename)
    root = tree.getroot()
    namespace = {"ns": "ec/tss.core"}
    logger.info(tree)

    # Number of series to adjust
    a = []
    for child in root:
        x = child.attrib
        y = list(x.values())
        a.append(y)
    NoOfSeries = len(a[1:len(a)])

    # Set new path
    for x in range(1, NoOfSeries):
        y = x - 1
        series = f"sa{x}"
        b2tf = root.findall(
            f"""./ns:item/[@name="{series}"]/ns:subset/ns:item/ns:ts/ns:metaData/""",
            namespace,
        )
        b2tgb = b2tf[2]
        input_path2 = input_path.replace("/", "%2F")
        newpath = f"""demetra://tsprovider/Xml/20111201/SERIES?file={input_path2}%2F{input_filename}#collectionIndex=0&seriesIndex={y}"""
        b2tgb.set("value", newpath)

    # Set source/filetype
    for x in range(1, NoOfSeries):
        y = x - 1
        series = f"sa{x}"
        src = root.findall(
            f"""./ns:item/[@name="{series}"]/ns:subset/ns:item/ns:ts/ns:metaData/""",
            namespace,
        )
        src2 = src[1]
        new_src = "Xml"
        src2.set("value", new_src)

    tree.write(f"{output_path}/{spec_filename}")
