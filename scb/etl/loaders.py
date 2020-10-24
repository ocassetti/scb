from pyspark.sql import SparkSession

SOURCE_TABLES = {"checkins", "ratings", "socialgraph", "users", "venues"}


def load_source(spark: SparkSession, db_url="jdbc:sqlite:resources/source_db/fsdata.db", write_mode="overwrite"):
    """
    Load all tables register them and save in raw as well
    :param spark:
    :param db_url:
    :parm write_mode
    :return: dictionary of SparkData
    """
    source_tables = SOURCE_TABLES

    source_df = {}
    for table in source_tables:
        source_df[table] = spark.read.format('jdbc').options(url=db_url, dbtable=table, driver='org.sqlite.JDBC').load()
        source_df[table].createOrReplaceTempView(table)
        source_df[table].write.format("orc").mode(write_mode).save(f"data/raw/{table}")

    return source_df
