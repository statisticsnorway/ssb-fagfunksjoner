import xml.etree.ElementTree as ET

from fagfunksjoner.fagfunksjoner_logger import logger


def change_input_to_xml(
    spec_path: str,
    spec_filename: str,
    input_path: str,
    input_filename: str,
    output_path: str,
) -> None:
    """Parse specification-files from Jdemetra+ and replace references to the old file format with XML-file references.

    Args:
        spec_path (str): Path to where your specification-files are located, typically in a folder called SAProcessing/.
        spec_filename (str): Name of specification-file. Default from Jdemetra+ is SAProcessing-1, SAProcessing-2, etc.
        input_path (str): Path to directory where input-files are located, e.g., ~/sa/project/input.
        input_filename (str): Name of input-file for the specification stated in the first two arguments.
        output_path (str): Path to where you want to save the new specification-files.

    """
    # Parse the specification file
    tree = ET.parse(f"{spec_path}/{spec_filename}")
    root = tree.getroot()
    namespace = {"ns": "ec/tss.core"}
    logger.info(tree)

    # Number of series to adjust
    series_list = [list(child.attrib.values()) for child in root]
    number_of_series = len(series_list[1:])

    # Set new path for each series
    input_path_encoded = input_path.replace("/", "%2F")
    for series_index in range(1, number_of_series):
        series_name = f"sa{series_index}"
        subset_item = root.findall(
            f"./ns:item[@name='{series_name}']/ns:subset/ns:item/ns:ts/ns:metaData/",
            namespace,
        )[2]
        new_path = (
            f"demetra://tsprovider/Xml/20111201/SERIES?file={input_path_encoded}%2F"
            f"{input_filename}#collectionIndex=0&seriesIndex={series_index - 1}"
        )
        subset_item.set("value", new_path)

    # Set source/file type for each series
    for series_index in range(1, number_of_series):
        series_name = f"sa{series_index}"
        metadata_items = root.findall(
            f"./ns:item[@name='{series_name}']/ns:subset/ns:item/ns:ts/ns:metaData/",
            namespace,
        )
        metadata_items[1].set("value", "Xml")

    # Save the modified tree to the output path
    tree.write(f"{output_path}/{spec_filename}")
