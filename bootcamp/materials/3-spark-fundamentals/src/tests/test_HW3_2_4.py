from pyspark.sql import SparkSession

def do_hosts_cumulated_transformation(spark, events_df, last_day_df, current_date):
    if hasattr(current_date, 'strftime'):
        current_date_str = current_date.strftime('%Y-%m-%d %H:%M:%S')
    else:
        current_date_str = str(current_date) 

    query = f"""
        WITH current_day_data AS (
            SELECT 
                host AS host_id,
                CAST(DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) AS STRING) AS today_date
            FROM events
            WHERE DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) = '{current_date_str}'
            GROUP BY host, DATE_TRUNC('day', CAST(event_time AS TIMESTAMP))
        ),
        updated_hosts AS (
            SELECT 
                ld.host_id,
                CASE 
                    WHEN cd.today_date IS NOT NULL THEN array_union(ld.host_activity_datelist, array(cd.today_date))
                    ELSE ld.host_activity_datelist
                END AS host_activity_datelist,
                '{current_date_str}' AS date
            FROM last_day_cumulated ld
            LEFT JOIN current_day_data cd
            ON ld.host_id = cd.host_id
        ),
        new_hosts AS (
            SELECT 
                cd.host_id,
                array(cd.today_date) AS host_activity_datelist,
                cd.today_date AS date
            FROM current_day_data cd
            LEFT JOIN last_day_cumulated ld
            ON cd.host_id = ld.host_id
            WHERE ld.host_id IS NULL
        )
        SELECT * FROM updated_hosts
        UNION ALL
        SELECT * FROM new_hosts;
    """
    events_df.createOrReplaceTempView("events")
    last_day_df.createOrReplaceTempView("last_day_cumulated")
    return spark.sql(query)

def main():
    current_date = '2023-01-31 00:00:00'

    spark = SparkSession.builder \
        .appName("Week3_HW_4_Host_Activity_Datelist") \
        .getOrCreate()
    try:
        events_df = spark.table("events")
        last_day_df = spark.table("hosts_cumulated")
        output_df = do_hosts_cumulated_transformation(spark, events_df, last_day_df, current_date)
        output_df.write.mode("overwrite").insertInto("hosts_cumulated")
        print(f"Host activity cumulation job for {current_date} completed successfully.")
    except Exception as e:
        print(f"Error in Host Activity Datelist job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()