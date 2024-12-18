from collections import namedtuple

from chispa.dataframe_comparer import assert_df_equality

from ..jobs.hosts_cumulated_job import transform_hosts

Events = namedtuple('Events', ['host', 'event_time'])
HostsCumulated = namedtuple('HostsCumulated', ['host', 'host_activity_datelist', 'current_host_date'])


def test_hosts_cumulated_generation(spark):
    source_data = [
        Events('www.eczachly.com', '2023-01-06 22:10:02.973000'),
        Events('www.eczachly.com', '2023-01-26 18:03:43.762000'),
        Events('www.eczachly.com', '2023-01-03 16:26:24.017000'),
        Events('www.zachwilson.tech', '2023-01-23 21:39:41.975000'),
        Events('admin.zachwilson.tech', '2023-01-09 05:16:48.598000')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = transform_hosts(spark, source_df)

    expected_data = [
        HostsCumulated('www.eczachly.com', ['2023-01-06', '2023-01-26', '2023-01-03'], '2023-01-31'),
        HostsCumulated('admin.zachwilson.tech', ['2023-01-09'], '2023-01-31'),
        HostsCumulated('www.zachwilson.tech', ['2023-01-23'], '2023-01-31')
    ]
    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
