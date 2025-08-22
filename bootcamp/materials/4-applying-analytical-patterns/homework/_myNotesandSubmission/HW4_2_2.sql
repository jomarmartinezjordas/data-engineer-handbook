-- DROP TABLE IF EXISTS game_details_dashboard;

CREATE TABLE game_details_dashboard AS
WITH game_details_augmented AS (
    SELECT
        gd.game_id,
        gd.team_id,
        gd.player_name,
        gd.pts,
        g.season,
        gd.team_abbreviation,
        gd.team_city,
        gd.plus_minus      
    FROM game_details gd
    LEFT JOIN games g ON gd.game_id = g.game_id
    WHERE gd.pts IS NOT NULL
),
team_wins AS (
    SELECT 
        team_abbreviation, 
        COUNT(*) AS total_wins
    FROM (
        SELECT 
            team_abbreviation,
            game_id
        FROM game_details
        GROUP BY game_id, team_abbreviation
        HAVING SUM(plus_minus) > 0
    ) won_games
    GROUP BY team_abbreviation
)
SELECT
    CASE
        WHEN GROUPING(gda.player_name) = 0 AND GROUPING(gda.team_abbreviation) = 0 THEN 'player_team'
        WHEN GROUPING(gda.player_name) = 0 AND GROUPING(gda.season) = 0 THEN 'player_season'
        WHEN GROUPING(gda.team_abbreviation) = 0 THEN 'team'
        ELSE 'overall'
    END AS aggregation_level,
    COALESCE(gda.player_name, '(overall)') AS player_name,
    COALESCE(gda.team_abbreviation, '(overall)') AS team_abbreviation,
    COALESCE(CAST(gda.season AS TEXT), '(overall)') AS season,
    SUM(gda.pts) AS total_points,
    MAX(tw.total_wins) AS total_wins
FROM game_details_augmented gda
LEFT JOIN team_wins tw ON gda.team_abbreviation = tw.team_abbreviation
GROUP BY GROUPING SETS (
    (gda.player_name, gda.team_abbreviation),
    (gda.player_name, gda.season),
    (gda.team_abbreviation)
)
ORDER BY total_points DESC;

-- Top scorers by team
SELECT
    player_name,
    total_points
FROM game_details_dashboard 
WHERE aggregation_level = 'player_team' 
ORDER BY total_points DESC;

-- Top scorers by season
SELECT
    player_name,
    total_points,
    season
FROM game_details_dashboard 
WHERE aggregation_level = 'player_season' 
ORDER BY total_points DESC;

-- Top winning teams
SELECT
    team_abbreviation AS team_name,
    total_wins
FROM game_details_dashboard 
WHERE aggregation_level = 'team'
ORDER BY total_wins DESC;