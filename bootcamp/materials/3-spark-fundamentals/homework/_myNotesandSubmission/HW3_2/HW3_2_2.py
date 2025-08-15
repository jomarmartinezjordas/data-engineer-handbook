from chispa.dataframe_comparer import *
from ..jobs.week3_HW_2_actor_history_scd_job import do_actors_history_scd_transformation
from collections import namedtuple

Actor = namedtuple("Actor", "actor actorid quality_class is_active current_year")
ActorsHistorySCD = namedtuple(
    "ActorsHistorySCD", "actor actorid streak_identifier quality_class is_active start_date end_date current_year"
)


def test_actors_history_scd(spark):
    input_data = [
        Actor("Coco Martin", 1, "star", True, 2020),
        Actor("Coco Martin", 1, "star", True, 2021),
        Actor("Coco Martin", 1, "legend", True, 2022),
        Actor("Julia Barretto", 2, "rising", True, 2020),
        Actor("Julia Barretto", 2, "star", True, 2021),
        Actor("Julia Barretto", 2, "star", True, 2022),
    ]
    expected_data = [
        ActorsHistorySCD("Coco Martin", 1, 0, "star", True, 2020, 2021, 2022),
        ActorsHistorySCD("Coco Martin", 1, 1, "legend", True, 2022, 2022, 2022),
        ActorsHistorySCD("Julia Barretto", 2, 0, "rising", True, 2020, 2020, 2022),
        ActorsHistorySCD("Julia Barretto", 2, 1, "star", True, 2021, 2022, 2022),
    ]
    input_df = spark.createDataFrame(input_data)
    expected_df = spark.createDataFrame(expected_data)
    actual_df = do_actors_history_scd_transformation(spark, input_df)
    assert_df_equality(actual_df, expected_df)