from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('spark-testing-homework')
        .getOrCreate()
    )

    transformed_df = transform_actors(spark, spark.table('actors'))
    transformed_df.write.mode('overwrite').insertInto('actors_history_scd')


def transform_actors(spark, df):
    df.createOrReplaceTempView('actors')
    query = ("""
        WITH
        max_year AS (
            SELECT
                MAX(current_year) AS max_year
            FROM
                actors
            LIMIT
                1
        ),
        -- ------------------------------
        previous_changes AS (
            SELECT
                actor_id,
                actor,
                current_year,
                quality_class,
                is_active,
                (
                    LAG(quality_class) OVER (PARTITION BY actor_id ORDER BY current_year) <> quality_class 
                    OR LAG(is_active) OVER (PARTITION BY actor_id ORDER BY current_year) <> is_active
                ) AS was_change
            FROM
                actors
        ),
        -- ------------------------------
        streak_identified AS (
            SELECT
                actor_id,
                actor,
                current_year,
                quality_class,
                is_active,
                SUM(CAST(COALESCE(was_change, FALSE) AS INTEGER)) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_id
            FROM
                previous_changes
        ),
        -- ------------------------------
        streak_aggregated AS (
            SELECT
                actor_id,
                actor,
                quality_class,
                is_active,
                streak_id,
                MIN(current_year) AS start_year,
                MAX(current_year) AS end_year
            FROM
                streak_identified
            GROUP BY
                actor_id,
                actor,
                quality_class,
                is_active,
                streak_id
        )
        -- -----------------------------
        SELECT
            actor_id,
            actor,
            quality_class,
            is_active,
            (SELECT max_year FROM max_year) AS current_year,
            start_year,
            end_year
        FROM
            streak_aggregated
    """)  # noqa

    transformed_df = spark.sql(query)
    return transformed_df
