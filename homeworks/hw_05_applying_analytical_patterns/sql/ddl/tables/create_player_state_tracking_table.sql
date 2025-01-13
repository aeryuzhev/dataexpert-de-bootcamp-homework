DROP TABLE IF EXISTS player_state_tracking;
-- --------------------------------------------------------------------------------------
CREATE TABLE players_state_tracking (
    player_name          TEXT,
    first_active_season  INTEGER,
    last_active_season   INTEGER,
    season_state         season_state_type,
    current_season       INTEGER,
    -- ------------------------------
    PRIMARY KEY (player_name, current_season)
);
