import re
import gzip
import json
import requests
import threading
from warcio.archiveiterator import ArchiveIterator

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from concurrent.futures import ThreadPoolExecutor, as_completed

COMMON_CRAWL_INDEX_URL = "https://index.commoncrawl.org/collinfo.json"


def get_commoncrawl_2025_indexes():
    print({"fetching commmoncrawl Indexes"})
    resp = requests.get(COMMON_CRAWL_INDEX_URL)
    matches = re.findall(r"(CC-MAIN-2025-\d+)", resp.text)
    print({"Total fetched commmoncrawl Indexes"},sorted(set(matches)))
    return sorted(set(matches))


def fetch_single_wat(wat_url, limit, collected_len, done_event):
    print("Started fetching records for wat url ->", wat_url)
    records = []
    try:
        stream = requests.get(wat_url, stream=True, timeout=30)
        for record in ArchiveIterator(stream.raw, arc2warc=True):
            if done_event.is_set():
                break

            if record.rec_type != "metadata":
                continue

            payload = record.content_stream().read()
            data = json.loads(payload)

            url = data.get("Envelope", {}).get("WARC-Header-Metadata", {}).get("WARC-Target-URI", "")
            if ".au" not in url:
                continue

            meta = data.get("Envelope", {}).get("Payload-Metadata", {}).get("HTTP-Response-Metadata", {}).get("HTML-Metadata", {})
            title = meta.get("Head", {}).get("Title", "")
            metas = meta.get("Head", {}).get("Metas", [])

            company_name = title or ""
            industry = ""
            for m in metas:
                if m.get("name", "").lower() in ("description", "industry"):
                    industry = m.get("content", "")
                    break

            records.append({
                "url": url,
                "company_name": company_name.strip(),
                "industry": industry.strip()
            })

            # Check soft limit inside loop
            if collected_len + len(records) >= limit:
                break

    except Exception as e:
        print(f"Failed to process {wat_url}: {e}")
    return records


def trigger_optimize_commoncrawl_parallel_fetch(fetch_single_wat, get_commoncrawl_2025_indexes,
                                        COMMON_CRAWL_WEB, DECODE, limit=300000, spark=None, max_threads=10):
    if spark is None:
        spark = (SparkSession.builder
                 .appName("CommonCrawlIngestionOptimized")
                 .config("spark.jars", "/path/to/postgresql-<version>.jar")
                 .getOrCreate())

    results = []
    results_lock = threading.Lock()
    done_event = threading.Event()

    indexes = get_commoncrawl_2025_indexes()
    wat_urls = []

    for index in indexes:
        wat_list_url = f"{COMMON_CRAWL_WEB}crawl-data/{index}/wat.paths.gz"
        print("fetching wat_list_url->",wat_list_url )
        try:
            resp = requests.get(wat_list_url)
            resp.raise_for_status()
            paths = gzip.decompress(resp.content).decode(DECODE).splitlines()
            wat_urls.extend([f"{COMMON_CRAWL_WEB}{path}" for path in paths])
        except Exception:
            continue

    print("Total Wat URLs available ->", len(wat_urls))

    def safe_fetch(url):
        if done_event.is_set():
            return []

        with results_lock:
            collected_len = len(results)
            if collected_len >= limit:
                done_event.set()
                return []

        # Fetch without holding the lock
        new_records = fetch_single_wat(url, limit, collected_len, done_event)

        if not new_records:
            return []

        with results_lock:
            available_space = limit - len(results)
            if available_space <= 0:
                done_event.set()
                return []

            accepted_records = new_records[:available_space]
            results.extend(accepted_records)

            if len(results) >= limit:
                done_event.set()

        # Write only accepted records to DB
        try:
            df = spark.createDataFrame(accepted_records)
            df.write.format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/aus_comp_db") \
                .option("dbtable", "core_db.commoncrawl_companies") \
                .option("user", "postgres") \
                .option("password", "Seneca@12345") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append").save()
        except Exception as e:
            print(f"DB write failed for {url}: {e}")

        return accepted_records

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(safe_fetch, url): url for url in wat_urls}
        for future in as_completed(futures):
            new_records = future.result()
            if not new_records:
                continue
            print("fetched records count in results-> ", len(results))
            with results_lock:
                results.extend(new_records)
                if len(results) >= limit:
                    done_event.set()
                    break
                # Incremental write
                df = spark.createDataFrame(results[-len(new_records):])
                print("writing to db")
                df.write.format("jdbc") \
                    .option("url", "jdbc:postgresql://localhost:5432/aus_comp_db") \
                    .option("dbtable", "core_db.commoncrawl_companies") \
                    .option("user", "postgres") \
                    .option("password", "Seneca@12345") \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append").save()
                print("Finished writing ")

    if results:
        return spark.createDataFrame(results[:limit])
    else:
        print("No results collected — returning empty DataFrame with defined schema.")
        empty_schema = StructType([
            StructField("url", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("industry", StringType(), True),
        ])
        return spark.createDataFrame([], schema=empty_schema)


def optimize_commoncrawl_parallel_fetch(limit=50000, spark=None):
    print("Initiated the process for parallel fetch and batch writing")
    return trigger_optimize_commoncrawl_parallel_fetch(
        fetch_single_wat=fetch_single_wat,
        get_commoncrawl_2025_indexes=get_commoncrawl_2025_indexes,
        COMMON_CRAWL_WEB="https://data.commoncrawl.org/",
        DECODE="utf-8",
        limit=limit,
        spark=spark,
        max_threads=10
    )
