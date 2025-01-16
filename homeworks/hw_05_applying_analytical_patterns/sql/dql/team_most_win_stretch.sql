WITH
games_data AS (
    SELECT DISTINCT
        g.game_date_est AS game_date,
        gd.team_abbreviation AS team,
        CASE 
            WHEN g.home_team_wins = 1 AND gd.team_id = g.home_team_id THEN 1
            WHEN g.home_team_wins = 0 AND gd.team_id = g.visitor_team_id THEN 1
            ELSE 0
        END AS is_winner
    FROM
        games g
        JOIN game_details gd ON gd.game_id = g.game_id
),
-- ------------------------------
games_stretch AS (
    SELECT
        team,
        game_date,
        is_winner,
        SUM(is_winner) OVER (
            PARTITION BY team 
            ORDER BY game_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) won_games_stretch
    FROM
        games_data
)
-- ------------------------------
SELECT
    team,
    MAX(won_games_stretch) AS max_won_games_stretch
FROM
    games_stretch
GROUP BY
    team
ORDER BY
    MAX(won_games_stretch) DESC
LIMIT
    1
