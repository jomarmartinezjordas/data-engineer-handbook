from pyspark.sql import SparkSession

query = """
    WITH with_previous AS (
        SELECT 
            actor,
            actorid,
            quality_class,
            is_active,
            current_year,
            LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
            LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
        FROM actors
    ),
    with_indicators AS (
        SELECT *,
            CASE
                WHEN quality_class <> previous_quality_class THEN 1
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
            END AS change_indicator
        FROM with_previous
    ),
    with_streaks AS (
        SELECT *,
            SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
        FROM with_indicators
    )
    SELECT
        actor,
        actorid,
        streak_identifier,
        quality_class,
        is_active,
        MIN(current_year) AS start_date,
        MAX(current_year) AS end_date,
        (SELECT MAX(current_year) FROM actors) AS current_year
    FROM with_streaks
    GROUP BY actor, actorid, streak_identifier, quality_class, is_active
    ORDER BY actorid;
    """

def do_actors_history_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .appName("Week3_HW_2_Actor_History_SCD") \
        .getOrCreate()
    try:
        actors_df = spark.table("actors")
        output_df = do_actors_history_scd_transformation(spark, actors_df)
        output_df.write.mode("overwrite").insertInto("actors_history_scd")
        print("Actor SCD job completed successfully.")
    except Exception as e:
        print(f"Error in Actor SCD job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()