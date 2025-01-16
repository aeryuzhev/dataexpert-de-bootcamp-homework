DROP TABLE IF EXISTS game_details_aggregated;
-- --------------------------------------------------------------------------------------
CREATE TABLE game_details_aggregated (
    aggregation_level  TEXT,
    season             TEXT,
    team               TEXT,
    player             TEXT,
    total_wins         INTEGER,
    total_points       INTEGER
)
