INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date)
WITH ranked_actors AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_quality_class,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_is_active
    FROM actors
),
changes AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year AS start_date,
        LEAD(current_year) OVER (PARTITION BY actorid ORDER BY current_year) - 1 AS end_date_raw
    FROM ranked_actors
    WHERE
        current_year = (SELECT MIN(current_year) FROM actors) OR
        quality_class IS DISTINCT FROM prev_quality_class OR
        is_active IS DISTINCT FROM prev_is_active
)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    COALESCE(end_date_raw, 9999) AS end_date
FROM changes;