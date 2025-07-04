import zipfile
import pyspark
import requests

from lxml import etree
from io import BytesIO
from pyspark.sql import SparkSession

# Constants
DECODE = "utf-8"
COMMON_CRAWL_WEB = "https://data.commoncrawl.org/"
COMMON_CRAWL_INDEX_URL = "https://data.commoncrawl.org/crawl-data/index.html"


def parse_abr_xml(source, spark) -> pyspark.sql.DataFrame:
    """
    Parses ABR XML files from a ZIP (URL) and returns a unified Spark DataFrame.

    Args:
        source (str): Local file path or HTTP(S) URL to a .zip file containing XMLs.
        spark (SparkSession): Active Spark session.

    Returns:
        pyspark.sql.DataFrame: Parsed ABR data from all XMLs as a Spark DataFrame.
    """
    zip_data = BytesIO()

    # Step 1: Load ZIP from URL
    if source.startswith("http://") or source.startswith("https://"):
        print(f"🔄 Downloading ABR ZIP from {source} ...")
        response = requests.get(source)
        response.raise_for_status()
        zip_data = BytesIO(response.content)

    records = []
    with zipfile.ZipFile(zip_data) as z:
        xml_files = [f for f in z.namelist() if f.endswith(".xml")]

        print(f"📁 Found {len(xml_files)} XML files in ZIP.")
        for xml_file in xml_files:
            print(f"📄 Parsing {xml_file} ...")
            with z.open(xml_file) as file:
                tree = etree.parse(file)
                root = tree.getroot()

                for abr in root.findall(".//ABR"):
                    abn = abr.findtext("ABN", default="").strip()
                    status = abr.find("ABN").attrib.get("status", "").strip()
                    start_date = abr.find("ABN").attrib.get("ABNStatusFromDate", "").strip()

                    entity_type = abr.findtext("EntityType/EntityTypeText", default="").strip()

                    name = abr.findtext("MainEntity/NonIndividualName/NonIndividualNameText", default="").strip()

                    address_node = abr.find("MainEntity/BusinessAddress/AddressDetails")
                    if address_node is not None:
                        postcode = address_node.findtext("Postcode", default="").strip()
                        state = address_node.findtext("State", default="").strip()
                        address = ""  # extra
                    else:
                        address, postcode, state = "", "", ""

                    records.append({
                        "abn": abn,
                        "entity_name": name,
                        "entity_type": entity_type,
                        "entity_status": status,
                        "entity_address": address,
                        "entity_postcode": postcode,
                        "entity_state": state,
                        "entity_start_date": start_date
                    })

    if not records:
        print("⚠️ No valid <ABR> records found. Returning empty DataFrame.")
        return spark.createDataFrame([],
                                     schema="abn STRING, entity_name STRING, entity_type STRING, entity_status STRING, entity_address STRING, entity_postcode STRING, entity_state STRING, entity_start_date STRING")

    print(f"✅ Parsed {len(records)} business records.")
    return spark.createDataFrame(records)

# Need to modularize fetch commoncrawl and put it in this file
