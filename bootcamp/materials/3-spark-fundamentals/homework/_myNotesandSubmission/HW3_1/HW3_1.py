from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, sum, countDistinct, col, avg, desc, first


def main():
    """Main function to execute the Spark job for DataExpert.io assignment."""

    spark = SparkSession.builder \
        .appName("DataExpert Spark Fundamentals - Refactored") \
        .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    print("Disabled automatic broadcast join.")

    print("Loading data...")
    match_details = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/match_details.csv")
    matches = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/matches.csv")
    medals_matches_players = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals_matches_players.csv")
    medals = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals.csv")
    maps = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/maps.csv")
    print("Data loaded.")

    print("Broadcasting medals and maps...")
    broadcasted_medals = broadcast(medals.alias("med"))
    broadcasted_maps = broadcast(maps.alias("map"))
    print("Broadcasting done.")

    try:
        print("Dropping existing bucketed tables (if any)...")
        spark.sql("DROP TABLE IF EXISTS bootcamp.bucketed_matches")
        spark.sql("DROP TABLE IF EXISTS bootcamp.bucketed_match_details")
        spark.sql("DROP TABLE IF EXISTS bootcamp.bucketed_medals_matches_players")
        print("Existing tables dropped.")
    except Exception as e:
        print(f"Warning: Could not drop tables (might not exist): {e}")

    print("Creating bucketed tables on match_id (16 buckets)...")
    matches.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.bucketed_matches")
    match_details.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.bucketed_match_details")
    medals_matches_players.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.bucketed_medals_matches_players")
    print("Bucketed tables created.")

    print("Reading bucketed tables...")
    bucketed_matches = spark.read.table("bootcamp.bucketed_matches")
    bucketed_match_details = spark.read.table("bootcamp.bucketed_match_details")
    bucketed_medals_matches_players = spark.read.table("bootcamp.bucketed_medals_matches_players")
    print("Bucketed tables read.")

    # --- Aggregation and Analysis ---
    print("Performing aggregations and analysis...")

    # --- Question 1: Which player averages the most kills per game? ---
    print("Calculating player with highest average kills per game...")
    player_kills_avg = bucketed_match_details.groupBy("player_gamertag") \
        .agg(avg("player_total_kills").alias("avg_kills_per_game")) \
        .orderBy(desc("avg_kills_per_game")) \
        .limit(10)
    print("Top players by average kills:")
    player_kills_avg.show()

    # --- Question 2: Which playlist gets played the most? ---
    print("Calculating most played playlist...")
    playlist_popularity = bucketed_matches.groupBy("playlist_id") \
        .agg(countDistinct("match_id").alias("matches_played")) \
        .orderBy(desc("matches_played"))
    print("Most popular playlists:")
    playlist_popularity.show()

    # --- Question 3: Which map gets played the most? ---
    print("Calculating most played map...")
    matches_with_map_name = bucketed_matches.join(broadcasted_maps, "mapid", "left")
    map_popularity = matches_with_map_name.groupBy("mapid", "name") \
        .agg(countDistinct("match_id").alias("matches_played")) \
        .orderBy(desc("matches_played"))
    print("Most popular maps:")
    map_popularity.show()

    # --- Question 4: Which map do players get the most Killing Spree medals on? ---
    print("Calculating map with most Killing Spree medals...")
    try:
        killing_spree_medal_id_row = broadcasted_medals.filter(col("name") == "Killing Spree").select("medal_id").first()
        if killing_spree_medal_id_row is None:
             print("Warning: 'Killing Spree' medal not found. Skipping this aggregation.")
             most_killing_spree_medals = spark.createDataFrame([], schema="mapid string, name string, killing_spree_count long")
        else:
            killing_spree_medal_id = killing_spree_medal_id_row["medal_id"]
            print(f"Found 'Killing Spree' medal ID: {killing_spree_medal_id}")

            killing_spree_events = bucketed_medals_matches_players.filter(col("medal_id") == killing_spree_medal_id)

            killing_spree_with_matches = killing_spree_events.join(bucketed_matches, "match_id", "inner") # Inner join is fine here

            killing_spree_with_map_name = killing_spree_with_matches.join(broadcasted_maps, "mapid", "left")

            most_killing_spree_medals = killing_spree_with_map_name.groupBy("mapid", "name") \
                .agg(sum("count").alias("killing_spree_count")) \
                .orderBy(desc("killing_spree_count"))
    except Exception as e:
        print(f"Error calculating Killing Spree medals: {e}")
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        schema = StructType([
            StructField("mapid", StringType(), True),
            StructField("name", StringType(), True),
            StructField("killing_spree_count", LongType(), True)
        ])
        most_killing_spree_medals = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    print("Maps with most Killing Spree medals:")
    most_killing_spree_medals.show()

