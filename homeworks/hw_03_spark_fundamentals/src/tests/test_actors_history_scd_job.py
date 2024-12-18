from collections import namedtuple

from chispa.dataframe_comparer import assert_df_equality

from ..jobs.actors_history_scd_job import transform_hosts

Actors = namedtuple(
    'Actors',
    ['actor_id', 'actor', 'films', 'quality_class', 'current_year', 'is_active']
)
ActorsHistoryScd = namedtuple(
    'ActorsScd',
    ['actor_id', 'actor', 'quality_class', 'is_active', 'current_year', 'start_year', 'end_year']
)


def test_actors_history_scd_generation(spark):
    source_data = [
        Actors(
            'nm0000002',
            'Lauren Bacall',
            [
                ("Murder on the Orient Express", 56620, 7.3, "tt0071877"),
                ("The Shootist", 22409, 7.6, "tt0075213"),
                ("HealtH", 693, 5.7, "tt0079256")
            ],
            'bad',
            1980,
            'true'
        ),
        Actors(
            'nm0000002',
            'Lauren Bacall',
            [
                ("Murder on the Orient Express", 56620, 7.3, "tt0071877"),
                ("The Shootist", 22409, 7.6, "tt0075213"),
                ("HealtH", 693, 5.7, "tt0079256"),
                ("The Fan", 2038, 5.8, "tt0082362")
            ],
            'bad',
            1981,
            'true'
        ),
        Actors(
            'nm0000002',
            'Lauren Bacall',
            [
                ("Murder on the Orient Express", 56620, 7.3, "tt0071877"),
                ("The Shootist", 22409, 7.6, "tt0075213"),
                ("HealtH", 693, 5.7, "tt0079256"),
                ("The Fan", 2038, 5.8, "tt0082362"),
                ("The Greatest Movie Ever", 999999, 10.0, "tt0082334")
            ],
            'good',
            1982,
            'true'
        )
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = transform_hosts(spark, source_df)

    expected_data = [
        ActorsHistoryScd('nm0000002', 'Lauren Bacall', 'bad', 'true', 1982, 1980, 1981),
        ActorsHistoryScd('nm0000002', 'Lauren Bacall', 'good', 'true', 1982, 1982, 1982)
    ]
    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df, expected_df)
