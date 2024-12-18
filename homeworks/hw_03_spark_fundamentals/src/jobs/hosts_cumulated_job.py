from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('spark-testing-homework')
        .getOrCreate()
    )

    transformed_df = transform_hosts(spark, spark.table('events_deduplicated'))
    transformed_df.write.mode('overwrite').insertInto('hosts_cumulated')


def transform_hosts(spark, df):
    df.createOrReplaceTempView('events_deduplicated')

    query = ("""
        SELECT
            host,
            ARRAY_AGG(DISTINCT CAST(DATE(event_time) AS STRING)) AS host_activity_datelist,
            '2023-01-31' AS current_host_date
        FROM
            events_deduplicated
        WHERE
            DATE(event_time) <= '2023-01-31'
            AND host IS NOT NULL
        GROUP BY
            host
    """)

    transformed_df = spark.sql(query)
    return transformed_df
