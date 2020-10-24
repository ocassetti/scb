from pyspark import SparkConf
from pyspark.sql import SparkSession


class BasePipeline:
    """
    Simple pipeline for loading and transforming
    """

    def __init__(self, spark=None):
        if spark is None:
            conf = SparkConf().setMaster("local[4]").setAppName("etl").set("spark.scheduler.mode", "FAIR").set(
                "spark.jars", "resources/jars/sqlite-jdbc-3.32.3.2.jar")
            conf = conf.set("spark.driver.memory", "2g")

            self.spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            self.spark = spark
