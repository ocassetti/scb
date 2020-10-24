import pygeohash as pgh
from pyspark.sql.types import StringType, DoubleType


def encode_geohash(lat, lon):
    return pgh.encode(lat, lon, 4)


def approx_dist(a_lat, a_long, b_lat, b_long):
    a = pgh.encode(a_lat, a_long)
    b = pgh.encode(b_lat, b_long)
    return pgh.geohash_approximate_distance(a, b) / 1000


def geo_utils_register(spark):
    spark.udf.register("encode_geohash", encode_geohash, StringType())
    spark.udf.register("approx_dist", approx_dist, DoubleType())
