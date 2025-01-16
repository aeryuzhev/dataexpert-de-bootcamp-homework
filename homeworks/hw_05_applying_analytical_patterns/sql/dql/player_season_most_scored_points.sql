SELECT
    player,
    season,
    total_points
FROM
    game_details_aggregated
WHERE
    aggregation_level = 'player__season'
ORDER BY
    total_points DESC
LIMIT
    1;
