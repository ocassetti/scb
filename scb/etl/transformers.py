import enum

from pyspark.sql import SparkSession


class DerivedTables(enum.Enum):
    checkins_agg = "checkins_agg"
    social_graph_rollup = "social_graph_rollup"
    checkins_rollup = "checkins_rollup"
    profiles = "profiles"


def checkin_aggregation(spark: SparkSession):
    """

    :param spark: Spark session
    :return: DataFrame
    """
    df = spark.sql("""select
   c.id,
   c.user_id,
   venue_id,
   v.latitude,
   v.longitude,
   c.latitude c_latitude,
   c.longitude c_longitude,
   approx_dist(v.latitude, v.longitude , c.latitude, c.longitude) dst,
   encode_geohash(v.latitude, v.longitude) v_gh4,
   timestamp(created_at) as created_at,
   count(1) over(partition by user_id) total_chk,
   row_number() over(partition by user_id 
order by
   timestamp(created_at)) checkin_seq,
   count(1) over(partition by user_id, venue_id) total_venue_chk 
from
   checkins c 
   left join
      venues v 
      on v.id = c.venue_id
 """)
    df.createOrReplaceTempView(DerivedTables.checkins_agg.value)

    return df


def simple_profile(spark: SparkSession):
    """

    :param spark: Spark session
    :return: DataFrame
    """
    social_graph_rollup = spark.sql(
        "select first_user_id, collect_set(second_user_id) friends from socialgraph group by 1")
    social_graph_rollup.createOrReplaceTempView("social_graph_rollup")
    checkins_rollup = spark.sql("""select
   c.user_id,
   collect_list(struct(c.id, c.venue_id, c.latitude, c.longitude, c.c_latitude, 
                       c.c_longitude, c.created_at, r.rating, 
                       c.total_chk, c.checkin_seq, c.total_venue_chk, c.dst
                       )
                ) chks,
   count(1) total_chk,
   count(distinct c.venue_id) distinct_chk,
   approx_percentile(dst, 0.50) median_dst,
   approx_percentile(r.rating, 0.50) median_rating,
   approx_percentile(r.rating, 0.95) p95_rating 
from
   checkins_agg c 
   left join
      (
         select
            user_id,
            venue_id,
            avg(rating) rating 
         from
            ratings 
         group by
            1,
            2
      )
      r 
      on r.user_id = c.user_id 
      and r.venue_id = c.venue_id 
group by
   1 """)
    checkins_rollup.createOrReplaceTempView("checkins_rollup")
    profiles = spark.sql("""select u.id, u.latitude u_latitude, u.longitude u_longitude, encode_geohash(u.latitude, u.longitude) u_gh4,
    c.*, g.friends
    from users u 
    inner join checkins_rollup c on u.id = c.user_id
    left join social_graph_rollup g on u.id = g.first_user_id
    order by total_chk desc
    """)
    profiles.createOrReplaceTempView("profiles")
    profiles.write.format("orc").mode("overwrite").save("data/profiles")
    return profiles


def linked_profiles(spark: SparkSession):
    profiles_complete = spark.sql("""
    select
   p.*,
   g.friends_profile 
from
   profiles p 
   left join
      (
         select
            first_user_id user_id,
            collect_list(struct(p.user_id, p.chks)) friends_profile 
         from
            socialgraph g 
            inner join
               profiles p 
               on g.second_user_id = p.id 
         group by
            1 
      )
      g 
      on g.user_id = p.user_id

    """)
    profiles_complete.write.format("orc").mode("overwrite").save("data/profiles_complete")
    return profiles_complete


def simple_als(spark: SparkSession):
    """

    :param spark: Spark session
    :return: DataFrame
    """
    df = spark.sql(
        """select
   c.user_id,
   c.venue_id,
   c.v_gh4,
   max(c.total_venue_chk) total_venue_chk,
   avg(r.rating) rating 
from
   checkins_agg c 
   left join
      (
         select
            user_id,
            venue_id,
            avg(rating) rating 
         from
            ratings 
         group by
            1,
            2
      )
      r 
      on r.user_id = c.user_id 
      and r.venue_id = c.venue_id 
group by
   1,
   2,
   3""")
    df.write.format("orc").mode("overwrite").save("data/checkins")
