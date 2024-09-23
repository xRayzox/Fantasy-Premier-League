from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import datetime

# Get current date and time
now = datetime.datetime.now()

# Format the date and time as YYYY-MM-DD_HH-MM-SS for safe filename
formatted_datetime = now.strftime("%Y-%m-%d_%H-%M-%S")

def transform():
    spark = SparkSession.builder \
        .appName("FPL Data Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "ZAritWM0fgdiJNpV9VIj") \
        .config("spark.hadoop.fs.s3a.secret.key", "FBaOdH4ICewnDOZoLo0vGr39YUQMjkv4CdVIyjEA") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    input_path = "s3a://fpl/data/current_season.csv"
    
    # Define schema
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

    # Read the CSV file into a DataFrame
    spark_df = spark.read.csv(input_path, schema=schema, header=True)

    # Perform transformations
    spark_df = spark_df.withColumn('kickoff_time', F.to_timestamp(spark_df.kickoff_time))

    float_columns = ['bps', 'influence', 'creativity', 'threat', 'ict_index']
    for col in float_columns:
        spark_df = spark_df.withColumn(col, spark_df[col].cast(FloatType()))

    spark_df = spark_df.withColumn('value', (spark_df.value / 10).cast(FloatType())) \
                       .withColumnRenamed('round', 'GW') \
                       .withColumnRenamed('value', 'price') \
                       .withColumnRenamed('minutes', 'minutes_played')

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"  # Replace <host>, <port>, <database>
    properties = {
        "user": "airflow",  # Your PostgreSQL username
        "password": "airflow",  # Your PostgreSQL password
        "driver": "org.postgresql.Driver"  # Driver is specified here for clarity, but Spark handles this.
    }

    # Write the DataFrame to PostgreSQL
    spark_df.write.jdbc(url=jdbc_url, table="Fact_table", mode="overwrite", properties=properties)

if __name__ == "__main__":
    transform()
