from pyspark.sql import SparkSession

from container.steps.upsert import write_raw_to_postgres
from container.steps.ingest import parse_abr_xml
from container.steps.ingest_commoncrawl import optimize_commoncrawl_parallel_fetch

# constants
COMMON_CRAWL_INDEX = "CC-MAIN-2025-21"
COMMON_CRAWL_TABLE_RAW = "commoncrawl_companies"
ABR_DATA_EXTRACT_API = "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip"


def execute():
    """
    Initiates the etl process for aus_comp
    """

    # Step 1: Create a spark session
    spark = SparkSession.builder \
        .appName("FullPipeline") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # Step 2: Ingest data from Common Crawl

    # Ideal method
    # common_crawl_data = call_optimize_commoncrawl_parallel_fetch(limit=200000, spark=spark)
    # write_raw_to_postgres(common_crawl_data, table_name=COMMON_CRAWL_TABLE_RAW)

    # working solution
    optimize_commoncrawl_parallel_fetch(limit=200000, spark=spark)

    # Step 3: Ingest data from ABR
    url = ABR_DATA_EXTRACT_API
    abr_data = parse_abr_xml(url, spark)
    write_raw_to_postgres(abr_data, table_name="abr_entities")

    # Step 4: Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    execute()
