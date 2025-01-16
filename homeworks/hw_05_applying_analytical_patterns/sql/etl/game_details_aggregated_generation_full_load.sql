TRUNCATE TABLE game_details_aggregated;
-- --------------------------------------------------------------------------------------
INSERT INTO game_details_aggregated (
    WITH
    game_details_data AS (
        SELECT
            g.season,
            g.game_id,
            gd.team_abbreviation AS team,
            gd.player_name AS player,
            COALESCE(gd.pts, 0) AS points,
            CASE 
                WHEN g.home_team_wins = 1 AND gd.team_id = g.home_team_id THEN 1 
                WHEN g.home_team_wins = 0 AND gd.team_id = g.visitor_team_id THEN 1  
                ELSE 0
            END AS is_winner
        FROM
            games g
            JOIN game_details gd ON gd.game_id = g.game_id
    )
    -- ------------------------------
    SELECT
        CASE 
            WHEN GROUPING(player) = 0 AND GROUPING(team) = 0 THEN 'player__team'  
            WHEN GROUPING(player) = 0 AND GROUPING(season) = 0 THEN 'player__season'  
            WHEN GROUPING(team) = 0 THEN 'team' 
        END AS aggregation_level,
        COALESCE(season::TEXT, 'overall') AS season,
        COALESCE(team, 'overall') AS team,
        COALESCE(player, 'overall') AS player,
        COUNT(DISTINCT CASE WHEN is_winner = 1 THEN game_id ELSE NULL END) AS total_wins,
        SUM(points) AS total_points
    FROM
        game_details_data

    GROUP BY GROUPING SETS (
        (player, team),
        (player, season),
        (team)
    )
);
