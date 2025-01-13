DO $$
BEGIN
    FOR season IN 1996..2022 LOOP
        INSERT INTO players_state_tracking
            WITH
            last_season_records AS (
                SELECT
                    player_name,
                    first_active_season,
                    last_active_season,
                    season_state,
                    current_season
                FROM
                    players_state_tracking
                WHERE
                    current_season = season - 1 
            ),
            -- ------------------------------
            current_season_records AS (
                SELECT
                    player_name,
                    current_season
                FROM
                    players
                WHERE
                    current_season = season
                    AND is_active = TRUE
            )
            -- ------------------------------
            SELECT
                COALESCE(lsr.player_name, csr.player_name) AS player_name,
                COALESCE(lsr.first_active_season, csr.current_season) AS first_active_season,
                COALESCE(csr.current_season, lsr.last_active_season) AS last_active_season,
                CASE 
                    WHEN lsr.player_name IS NULL THEN 'New'
                    WHEN csr.current_season IS NULL AND lsr.last_active_season = lsr.current_season THEN 'Retired'
                    WHEN lsr.last_active_season = csr.current_season - 1 THEN 'Continued Playing'
                    WHEN csr.current_season - lsr.last_active_season > 1 THEN 'Returned from Retirement'
                    ELSE 'Stayed Retired'
                END::season_state_type AS season_state,
                season AS current_season
            FROM
                current_season_records csr
                FULL JOIN last_season_records lsr ON lsr.player_name = csr.player_name;
    END LOOP;
END;
$$
