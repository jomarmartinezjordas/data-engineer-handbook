WITH last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE end_date = %(current_year)s - 1
),
this_year_data AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active
    FROM actors
    WHERE current_year = %(current_year)s
),
scd_changes AS (
    SELECT
        COALESCE(t.actorid, l.actorid) AS actorid,
        COALESCE(t.actor, l.actor) AS actor,
        CASE
            WHEN t.actorid IS NULL THEN l.quality_class
            ELSE t.quality_class
        END AS quality_class,
        CASE
            WHEN t.actorid IS NULL THEN l.is_active
            ELSE t.is_active
        END AS is_active,
        CASE
            WHEN t.actorid IS NULL THEN l.start_date
            ELSE %(current_year)s
        END AS start_date,
        CASE
            WHEN t.actorid IS NULL THEN %(current_year)s - 1
            ELSE 9999
        END AS end_date
    FROM last_year_scd l
    FULL OUTER JOIN this_year_data t ON l.actorid = t.actorid
    WHERE
        t.actorid IS NOT NULL OR
        (t.actorid IS NULL AND l.end_date = %(current_year)s - 1)
),
new_records AS (
    SELECT
        t.actorid,
        t.actor,
        t.quality_class,
        t.is_active,
        %(current_year)s AS start_date,
        9999 AS end_date
    FROM this_year_data t
    WHERE NOT EXISTS (
        SELECT 1
        FROM actors_history_scd s
        WHERE s.actorid = t.actorid AND s.end_date = 9999
    )
)
INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date)
SELECT actorid, actor, quality_class, is_active, start_date, end_date FROM scd_changes
UNION ALL
SELECT actorid, actor, quality_class, is_active, start_date, end_date FROM new_records;