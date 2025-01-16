SELECT
    team,
    total_wins
FROM
    game_details_aggregated
WHERE
    aggregation_level = 'team'
ORDER BY
    total_wins DESC
LIMIT
    1;
