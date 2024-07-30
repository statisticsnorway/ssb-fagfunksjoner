import xml.etree.ElementTree as ET

import openpyxl

from fagfunksjoner.fagfunksjoner_logger import logger


def convert_excel_to_xml(file_name: str, sheet_name: str = "Ark1") -> None:
    """Convert an Excel sheet to an XML file.

    This function reads an Excel sheet specified by the sheet name and converts its contents
    to an XML format. Each cell in the sheet is represented as an XML element.

    Args:
        file_name (str): The name of the Excel file to read.
        sheet_name (str): The name of the sheet within the Excel file to convert to XML.

    Example:
        convert_excel_to_xml("example.xlsx", "Sheet1")
    """
    wb = openpyxl.load_workbook(file_name)
    sheet = wb[sheet_name]
    root = ET.Element("root")
    for row in sheet.rows:
        for cell in row:
            ET.SubElement(root, "cell", value=cell.value)
    tree = ET.ElementTree(root)
    path = f"{sheet_name}.xml"
    tree.write(path)
    logger.info("Output to %s", path)
