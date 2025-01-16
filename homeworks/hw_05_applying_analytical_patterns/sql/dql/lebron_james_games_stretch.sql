WITH
games_data AS (
    SELECT
        g.game_date_est AS game_date,
        gd.player_name,
        CASE WHEN COALESCE(gd.pts, 0) > 10 THEN 1 ELSE 0 END AS is_more_than_ten
    FROM
        games g
        JOIN game_details gd ON gd.game_id = g.game_id
    WHERE
        gd.player_name = 'LeBron James'
),
-- ------------------------------
games_grouped AS (
    SELECT
        game_date,
        player_name,
        is_more_than_ten,
        SUM(CASE WHEN is_more_than_ten = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY player_name
            ORDER BY game_date
        ) AS group_id 
    FROM
        games_data
),
-- ------------------------------
group_counts AS (
    SELECT
        group_id,
        COUNT(1) AS group_size
    FROM
        games_grouped
    WHERE
        is_more_than_ten = 1
    GROUP BY
        group_id
)
-- ------------------------------
SELECT
    MAX(group_size) AS max_games
FROM
    group_counts;
