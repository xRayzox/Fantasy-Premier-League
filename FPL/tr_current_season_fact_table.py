from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import logging

import os

def transform(input_path):
    spark = SparkSession.builder.appName('FPLDataTransformation') \
    .config("spark.sql.files.tmpDir", "/tmp/spark-tmp") \
    .config("spark.sql.warehouse.dir", "/opt/airflow/spark-warehouse") \
    .getOrCreate()
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("element", StringType(), False),
        StructField("fixture", StringType(), False),
        StructField("opponent_team", StringType(), False),
        StructField("total_points", IntegerType(), False),
        StructField("was_home", BooleanType(), False),
        StructField("kickoff_time", StringType(), False),
        StructField("team_h_score", IntegerType(), False),
        StructField("team_a_score", IntegerType(), False),
        StructField("round", StringType(), False),
        StructField("minutes", IntegerType(), False),
        StructField("goals_scored", IntegerType(), False),
        StructField("assists", IntegerType(), False),
        StructField("clean_sheets", IntegerType(), False),
        StructField("goals_conceded", IntegerType(), False),
        StructField("own_goals", IntegerType(), False),
        StructField("penalties_saved", IntegerType(), False),
        StructField("penalties_missed", IntegerType(), False),
        StructField("yellow_cards", IntegerType(), False),
        StructField("red_cards", IntegerType(), False),
        StructField("saves", IntegerType(), False),
        StructField("bonus", IntegerType(), False),
        StructField("bps", IntegerType(), False),
        StructField("influence", StringType(), False),
        StructField("creativity", StringType(), False),
        StructField("threat", StringType(), False),
        StructField("ict_index", StringType(), False),
        StructField("starts", IntegerType(), False),
        StructField("expected_goals", StringType(), False),
        StructField("expected_assists", StringType(), False),
        StructField("expected_goal_involvements", StringType(), False),
        StructField("expected_goals_conceded", StringType(), False),
        StructField("value", IntegerType(), False),
        StructField("transfers_balance", IntegerType(), False),
        StructField("selected", IntegerType(), False),
        StructField("transfers_in", IntegerType(), False),
        StructField("transfers_out", IntegerType(), False)
    ])

    # Read the CSV file into a DataFrame with the defined schema
    spark_df = spark.read.csv(input_path, schema=schema, header=True)

    # Example transformations
    spark_df = spark_df.withColumn('kickoff_time', F.to_timestamp(spark_df.kickoff_time))

    float_columns = ['bps', 'influence', 'creativity', 'threat', 'ict_index']
    for col in float_columns:
        spark_df = spark_df.withColumn(col, spark_df[col].cast(FloatType()))

    spark_df = spark_df.withColumn('value', (spark_df.value / 10).cast(FloatType())) \
                       .withColumnRenamed('round', 'GW') \
                       .withColumnRenamed('value', 'price') \
                       .withColumnRenamed('minutes', 'minutes_played')
    

    return spark_df



if __name__ == "__main__":
    import sys
    input_path = sys.argv[1]  # Pass input path via CLI argument
    transform(input_path)