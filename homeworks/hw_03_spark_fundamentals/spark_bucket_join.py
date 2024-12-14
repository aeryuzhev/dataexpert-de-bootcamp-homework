from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, broadcast, col, countDistinct, desc, sum
from pyspark.sql.types import IntegerType, LongType, TimestampType

NUMBER_OF_BUCKETS = 16
BUCKET_BY_FIELD = 'match_id'
MATCH_DETAILS_PATH = '/home/iceberg/data/match_details.csv'
MATCHES_PATH = '/home/iceberg/data/matches.csv'
MEDALS_MATCHES_PLAYERS_PATH = '/home/iceberg/data/medals_matches_players.csv'
MAPS_PATH = '/home/iceberg/data/maps.csv'
MEDALS_PATH = '/home/iceberg/data/medals.csv'

# Add properties
#     'spark.driver.memory      8g' 
#     'spark.executor.memory    8g' 
# to the /opt/spark/conf/spark-defaults.conf
spark = (
    SparkSession.builder
    .appName('spark-homework')
    .config('spark.sql.autoBroadcastJoinThreshold', '-1')
    .getOrCreate()
)


def read_csv(path: str) -> DataFrame:
    df = (
        spark.read
        .format('csv')
        .option('header', 'true')
        .option('delimiter', ',')
        .load(path)
    )
    return df


match_details_df = read_csv(MATCH_DETAILS_PATH)
matches_df = read_csv(MATCHES_PATH)
medals_matches_players_df = read_csv(MEDALS_MATCHES_PLAYERS_PATH)
maps_df = (
    read_csv(MAPS_PATH)
    .select(
        col('mapid'),
        col('name').alias('map_name')
    )
)
medals_df = (
    read_csv(MEDALS_PATH)
    .select(
        col('medal_id'),
        col('name').alias('medal_name')
    )
)

spark.sql('DROP TABLE IF EXISTS bootcamp.match_details_bucketed')
spark.sql('DROP TABLE IF EXISTS bootcamp.matches_bucketed')
spark.sql('DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed')


spark.sql(f"""
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id            STRING,
    player_gamertag     STRING,
    player_total_kills  INTEGER
)
CLUSTERED BY ({BUCKET_BY_FIELD}) INTO {NUMBER_OF_BUCKETS} BUCKETS
STORED AS PARQUET
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id         STRING,
    mapid            STRING,
    playlist_id      STRING,
    completion_date  TIMESTAMP
)
PARTITIONED BY (completion_date)
CLUSTERED BY ({BUCKET_BY_FIELD}) INTO {NUMBER_OF_BUCKETS} BUCKETS
STORED AS PARQUET
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
    match_id         STRING,
    player_gamertag  STRING,
    medal_id         LONG,
    count            INTEGER
)
CLUSTERED BY ({BUCKET_BY_FIELD}) INTO {NUMBER_OF_BUCKETS} BUCKETS
STORED AS PARQUET
""")

(
    match_details_df
    .select(
        col('match_id'),
        col('player_gamertag'),
        col('player_total_kills').cast(IntegerType())
    )
    .write
    .mode('append')
    .bucketBy(NUMBER_OF_BUCKETS, BUCKET_BY_FIELD)
    .saveAsTable('bootcamp.match_details_bucketed')
)

(
    matches_df
    .select(
        col('match_id'),
        col('mapid'),
        col('playlist_id'),
        col('completion_date').cast(TimestampType())
    )
    .write
    .mode('append')
    .partitionBy('completion_date')
    .bucketBy(NUMBER_OF_BUCKETS, BUCKET_BY_FIELD)
    .saveAsTable('bootcamp.matches_bucketed')
)

(
    medals_matches_players_df
    .select(
        col('match_id'),
        col('player_gamertag'),
        col('medal_id').cast(LongType()),
        col('count').cast(IntegerType())
    )
    .write
    .mode('append')
    .bucketBy(NUMBER_OF_BUCKETS, BUCKET_BY_FIELD)
    .saveAsTable('bootcamp.medals_matches_players_bucketed')
)

matches_bucketed_df = spark.table('bootcamp.matches_bucketed')
match_details_bucketed_df = spark.table('bootcamp.match_details_bucketed')
medals_matches_players_bucketed_df = spark.table('bootcamp.medals_matches_players_bucketed')

joined_df = (
    matches_bucketed_df.alias('m')
    .join(match_details_bucketed_df.alias('md'), col('m.match_id') == col('md.match_id'), 'inner')
    .join(medals_matches_players_bucketed_df.alias('mmp'), col('m.match_id') == col('mmp.match_id'), 'inner')
    .join(broadcast(maps_df.alias('map')), col('map.mapid') == col('m.mapid'), 'inner')
    .join(broadcast(medals_df.alias('mdl')), col('mdl.medal_id') == col('mmp.medal_id'), 'inner')
    .select(
        col('m.match_id'),
        col('m.completion_date'),
        col('md.player_gamertag'),
        col('md.player_total_kills'),
        col('map.map_name'),
        col('m.playlist_id'),
        col('mdl.medal_name'),
        col('mmp.count').alias('medal_count')
    )
)

most_kills_per_game_df = (
    joined_df
    .groupBy('player_gamertag')
    .agg(avg('player_total_kills').alias('avg_player_total_kills'))
    .orderBy(desc('avg_player_total_kills'))
)
most_kills_per_game_df.limit(1).show()

most_played_playlist_df = (
    joined_df
    .groupBy('playlist_id')
    .agg(countDistinct('match_id').alias('number_of_matches'))
    .orderBy(desc('number_of_matches'))
)
most_played_playlist_df.limit(1).show()

most_played_map_df = (
    joined_df
    .groupBy('map_name')
    .agg(countDistinct('match_id').alias('number_of_matches'))
    .orderBy(desc('number_of_matches'))
)
most_played_map_df.limit(1).show()

most_killing_spree_medals_df = (
    joined_df
    .filter(col('medal_name') == 'Killing Spree')
    .dropDuplicates(['match_id', 'map_name', 'medal_name'])
    .groupBy('map_name')
    .agg(sum('medal_count').alias('number_of_medals'))
    .orderBy(desc('number_of_medals'))
)
most_killing_spree_medals_df.limit(1).show()

matches_repartitioned_df = (
    matches_df
    .withColumn('completion_date', col('completion_date').cast(TimestampType()))
    .repartition(NUMBER_OF_BUCKETS, col('completion_date'))
)
matches_unsorted_df = matches_repartitioned_df
# Choose low cardibality columns
matches_sorted_df = (
    matches_repartitioned_df
    .sortWithinPartitions(
        col('completion_date'),
        col('playlist_id'),
        col('mapid')
    )
)

matches_unsorted_df.write.mode('overwrite').saveAsTable('bootcamp.matches_unsorted')
matches_sorted_df.write.mode('overwrite').saveAsTable('bootcamp.matches_sorted')

spark.sql("""
SELECT
    SUM(file_size_in_bytes) AS size,
    COUNT(1) AS num_files,
    'sorted'
FROM
    bootcamp.matches_sorted.files
UNION ALL
SELECT
    SUM(file_size_in_bytes) AS size,
    COUNT(1) AS num_files,
    'unsorted'
FROM
    bootcamp.matches_unsorted.files
""").show()

spark.stop()
