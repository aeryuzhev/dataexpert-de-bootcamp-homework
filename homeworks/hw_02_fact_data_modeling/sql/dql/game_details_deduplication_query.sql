WITH duplicate_identified AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS duplicate_id
    FROM
        game_details
)
-- ------------------------------
SELECT
    *
FROM
    duplicate_identified
WHERE
    duplicate_id = 1;
