-- Drop and recreate the tracking table
DROP TABLE IF EXISTS players_state_tracking;

CREATE TABLE players_state_tracking (
    player_name TEXT,
    first_active_season INTEGER,
    last_active_season INTEGER,
    season_state TEXT,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

CREATE OR REPLACE VIEW player_season_transitions AS
SELECT
    player_name,
    current_season,
    is_active,
    LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_is_active,
    LEAD(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS next_is_active
FROM players;

CREATE OR REPLACE FUNCTION get_season_state(
    is_active BOOLEAN,
    prev_is_active BOOLEAN,
    next_is_active BOOLEAN
) RETURNS TEXT AS $$
BEGIN
    RETURN CASE
        WHEN is_active AND prev_is_active IS NULL THEN 'New'
        WHEN NOT is_active AND prev_is_active THEN 'Retired'
        WHEN is_active AND prev_is_active THEN 'Continued Playing'
        WHEN is_active AND NOT prev_is_active THEN 'Returned from Retirement'
        WHEN NOT is_active AND NOT prev_is_active THEN 'Stayed Retired'
        ELSE 'Stale'
    END;
END;
$$ LANGUAGE plpgsql;

WITH previous_season AS (
    -- Get data from the previous tracking season
    SELECT *
    FROM players_state_tracking
    WHERE current_season = 2009
),
current_players AS (
    -- Get current season data with transition information
    SELECT *
    FROM player_season_transitions
    WHERE current_season = 2010
)
INSERT INTO players_state_tracking
SELECT
    COALESCE(curr.player_name, prev.player_name) AS player_name,
    COALESCE(prev.first_active_season, curr.current_season) AS first_active_season,
    CASE 
        WHEN curr.is_active THEN curr.current_season
        ELSE COALESCE(prev.last_active_season, 0)
    END AS last_active_season,
    get_season_state(
        COALESCE(curr.is_active, FALSE),
        curr.prev_is_active,
        curr.next_is_active
    ) AS season_state,
   COALESCE(curr.current_season, prev.current_season + 1) AS current_season
FROM current_players curr
FULL OUTER JOIN previous_season prev ON curr.player_name = prev.player_name;

SELECT * FROM players_state_tracking
WHERE player_name = 'Kevin Durant';