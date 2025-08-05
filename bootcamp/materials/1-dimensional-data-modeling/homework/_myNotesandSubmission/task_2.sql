INSERT INTO actors (actorid, actor, films, quality_class, is_active, current_year)
WITH yearly_films AS (
    SELECT
        af.actorid,
        af.actor,
        jsonb_agg(
            jsonb_build_object(
                'film', af.film,
                'votes', af.votes,
                'rating', af.rating,
                'filmid', af.filmid
            )
        ) AS films,
        MAX(af.year) AS latest_year,
        BOOL_OR(af.year = %(current_year)s) AS is_active
    FROM actor_films af
    WHERE af.year <= %(current_year)s
    GROUP BY af.actorid, af.actor
),
quality_ratings AS (
    SELECT
        af.actorid,
        CASE
            WHEN AVG(af.rating) > 8 THEN 'star'
            WHEN AVG(af.rating) > 7 THEN 'good'
            WHEN AVG(af.rating) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class
    FROM actor_films af
    WHERE af.year = %(current_year)s
    GROUP BY af.actorid
)
SELECT
    yf.actorid,
    yf.actor,
    yf.films,
    COALESCE(qr.quality_class, 'bad') AS quality_class,
    yf.is_active,
    %(current_year)s AS current_year
FROM yearly_films yf
LEFT JOIN quality_ratings qr ON yf.actorid = qr.actorid;