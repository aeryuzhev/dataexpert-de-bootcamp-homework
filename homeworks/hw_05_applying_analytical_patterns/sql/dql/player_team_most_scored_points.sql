SELECT
    player,
    team,
    total_points
FROM
    game_details_aggregated
WHERE
    aggregation_level = 'player__team'
ORDER BY
    total_points DESC
LIMIT
    1;
