-- Most wins in a 90-game stretch
WITH team_game_results AS (
    SELECT
        team_abbreviation,
        game_id,
        CASE WHEN SUM(plus_minus) > 0 THEN 1 ELSE 0 END AS win
    FROM game_details
    GROUP BY team_abbreviation, game_id
),
rolling_wins AS (
    SELECT
        tg.team_abbreviation,
        tg.game_id,
        SUM(tg.win) OVER (
            PARTITION BY tg.team_abbreviation
            ORDER BY g.game_date_est
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_last_90
    FROM team_game_results tg
    LEFT JOIN games g ON tg.game_id = g.game_id
)
SELECT  
    team_abbreviation, 
    MAX(wins_in_last_90) AS max_wins_in_90_games
FROM rolling_wins
GROUP BY team_abbreviation
ORDER BY max_wins_in_90_games DESC
LIMIT 1;

-- LeBron James consecutive games over 10 points
WITH lebron_game_points AS (
    SELECT
        gd.player_name,
        g.game_date_est,
        CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END AS above_10_points,
        ROW_NUMBER() OVER (ORDER BY g.game_date_est) AS game_sequence
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),
streak_groups AS (
    SELECT
        player_name,
        game_sequence - SUM(above_10_points) OVER (
            ORDER BY game_sequence
        ) AS streak_id
    FROM lebron_game_points
    WHERE above_10_points = 1
),
streak_counts AS (
    SELECT
        player_name,
        streak_id,
        COUNT(*) AS streak_length
    FROM streak_groups
    GROUP BY player_name, streak_id
)
SELECT 
    player_name,
    MAX(streak_length) AS longest_streak
FROM streak_counts
GROUP BY player_name;