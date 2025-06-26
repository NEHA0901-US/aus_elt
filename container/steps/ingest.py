import gzip
import json
import zipfile

import pyspark
import requests

from lxml import etree
from io import BytesIO
from pyspark.sql import SparkSession
from warcio.archiveiterator import ArchiveIterator

# Constants
DECODE = "utf-8"
COMMON_CRAWL_WEB = "https://data.commoncrawl.org/"


def fetch_commoncrawl_wat_records_spark(index="CC-MAIN-2025-10", limit=200000, spark=None) -> pyspark.sql.DataFrame:
    """
    Fetch WAT records from Common Crawl's March 2025 index with .au domains.

    Args:
        index (str): Index name.
        limit (int): Number of .au records to collect.
        spark (SparkSession): Optional SparkSession object.

    Returns:
        pyspark.sql.DataFrame: Spark DataFrame with website metadata.
    """
    if spark is None:
        spark = SparkSession.builder \
            .appName("CommonCrawlIngestion") \
            .getOrCreate()

    base_url = f"https://data.commoncrawl.org/crawl-data/{index}/wat.paths.gz"
    response = requests.get(base_url)
    response.raise_for_status()

    wat_paths = gzip.decompress(response.content).decode(DECODE).splitlines()
    results = []

    for path in wat_paths:
        wat_url = f"{COMMON_CRAWL_WEB}{path}"
        print(f"Fetching: {wat_url}")

        try:
            wat_stream = requests.get(wat_url, stream=True)
            for record in ArchiveIterator(wat_stream.raw, arc2warc=True):
                if record.rec_type != "metadata":
                    continue

                payload = record.content_stream().read()
                data = json.loads(payload)

                url = data.get("Envelope", {}).get("WARC-Header-Metadata", {}).get("WARC-Target-URI", "")
                if ".au" not in url:
                    continue

                meta = data.get("Envelope", {}).get("Payload-Metadata", {}).get("HTTP-Response-Metadata", {}).get(
                    "HTML-Metadata", {})
                title = meta.get("Head", {}).get("Title", "")
                metas = meta.get("Head", {}).get("Metas", [])

                company_name = title or ""
                industry = ""

                for m in metas:
                    if m.get("name", "").lower() in ("description", "industry"):
                        industry = m.get("content", "")
                        break

                results.append({
                    "url": url,
                    "company_name": company_name.strip(),
                    "industry": industry.strip()
                })

                if len(results) >= limit:
                    return spark.createDataFrame(results)

        except Exception as e:
            print(f"Failed to process {wat_url}: {e}")
            continue

    return spark.createDataFrame(results)


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
        return spark.createDataFrame([], schema="abn STRING, entity_name STRING, entity_type STRING, entity_status STRING, entity_address STRING, entity_postcode STRING, entity_state STRING, entity_start_date STRING")

    print(f"✅ Parsed {len(records)} business records.")
    return spark.createDataFrame(records)