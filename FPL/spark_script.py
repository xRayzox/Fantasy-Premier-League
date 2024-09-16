import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType*
import sys
# Add the path to your functions file (adjust if needed)
sys.path.append('/opt/airflow/fpl_functions')
from Functions import get_fpl_data, get_fixtures_data, get_players_history

def transform_fpl_teams_data(pandas_df):
    # Initialize Spark session
    spark = SparkSession.builder.appName("FPL_Teams_Transformation").getOrCreate()
    
    # Define schema
    schema = StructType([
        StructField("code", IntegerType(), True),
        StructField("draw", IntegerType(), True),
        StructField("form", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("loss", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("played", IntegerType(), True),
        StructField("points", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("short_name", StringType(), True),
        StructField("strength", IntegerType(), True),
        StructField("team_division", StringType(), True),
        StructField("unavailable", BooleanType(), True),
        StructField("win", IntegerType(), True),
        StructField("strength_overall_home", IntegerType(), True),
        StructField("strength_overall_away", IntegerType(), True),
        StructField("strength_attack_home", IntegerType(), True),
        StructField("strength_attack_away", IntegerType(), True),
        StructField("strength_defence_home", IntegerType(), True),
        StructField("strength_defence_away", IntegerType(), True),
        StructField("pulse_id", IntegerType(), True)
    ])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df, schema=schema)

    # Example transformation: Add a column for average strength
    spark_df = spark_df.withColumn(
        "average_strength", 
        (col("strength_overall_home") + col("strength_overall_away")) / 2
    )
    
    return spark_df

if __name__ == "__main__":
    teams_data = get_fpl_data()['teams']
    sui= json.dumps(teams_data, indent=4)
    # Convert to Pandas DataFrame
    pandas_df = pd.DataFrame(sui)
    
    # Transform the data
    teams_data_df = transform_fpl_teams_data(pandas_df)


    # Stop the Spark session
    spark.stop()
