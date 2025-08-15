from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from collections import namedtuple
from ..jobs.week3_HW_4_host_activity_datelist_job import do_hosts_cumulated_transformation

HostEvent = namedtuple("HostEvent", "host event_time")
HostCumulated = namedtuple("HostCumulated", "host_id host_activity_datelist date")

def test_hosts_cumulated_transformation(spark):
    current_date = '2023-01-31 00:00:00'
    events_data = [
        HostEvent("host1", "2023-01-31 10:00:00"),
        HostEvent("host2", "2023-01-31 11:00:00"),
    ]

    events_df = spark.createDataFrame(events_data)
    last_day_data = [
        HostCumulated("host1", ["2023-01-30 00:00:00"], "2023-01-30 00:00:00"),
    ]
    last_day_df = spark.createDataFrame(last_day_data)

    expected_data = [
        HostCumulated("host1", ["2023-01-30 00:00:00", "2023-01-31 00:00:00"], "2023-01-31 00:00:00"),
        HostCumulated("host2", ["2023-01-31 00:00:00"], "2023-01-31 00:00:00"),
    ]
    expected_df = spark.createDataFrame(expected_data)
    actual_df = do_hosts_cumulated_transformation(spark, events_df, last_day_df, current_date)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)