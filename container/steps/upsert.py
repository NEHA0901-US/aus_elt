from container.steps.db_conn import POSTGRES_CONFIG


def write_raw_to_postgres(df, table_name, mode="overwrite"):
    """
    Generic function to write a Spark DataFrame to PostgreSQL.
    """
    full_table = f"{POSTGRES_CONFIG['schema']}.{table_name}"

    df.repartition(150).write \
        .mode("append") \
        .format("jdbc") \
        .option("url", POSTGRES_CONFIG["url"]) \
        .option("dbtable", full_table) \
        .option("user", POSTGRES_CONFIG["user"]) \
        .option("password", POSTGRES_CONFIG["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 1000) \
        .option("numPartitions", 150) \
        .option("truncate", "false") \
        .save()

    print(f"✅ Data written to PostgreSQL table: {full_table}")
