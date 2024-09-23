from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# Get current date and time
now = datetime.datetime.now()

# Format the date and time as YYYY-MM-DD_HH-MM-SS for safe filename
formatted_datetime = now.strftime("%Y-%m-%d_%H-%M-%S")

def transform_previous_season():
    spark = SparkSession.builder \
        .appName("FPL Previous Season Data Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "ZAritWM0fgdiJNpV9VIj") \
        .config("spark.hadoop.fs.s3a.secret.key", "FBaOdH4ICewnDOZoLo0vGr39YUQMjkv4CdVIyjEA") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Load previous season data from MinIO
    previous_season_path = "s3a://fpl/data/previous_seasons.csv"

    # Define schema for the previous seasons DataFrame
    schema = StructType([
        StructField("season_name", StringType(), False),
        StructField("element_code", StringType(), False),
        StructField("start_cost", FloatType(), False),
        StructField("end_cost", FloatType(), False),
        StructField("total_points", IntegerType(), False),
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
        StructField("bonus", IntegerType(), False)
    ])

    # Read the CSV file into a DataFrame
    previous_df = spark.read.csv(previous_season_path, schema=schema, header=True)

    # Convert costs from integer to float (e.g., 55 to 5.5)
    previous_df = previous_df.withColumn('start_cost', previous_df['start_cost'] / 10) \
                             .withColumn('end_cost', previous_df['end_cost'] / 10)

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"  # Replace <host>, <port>, <database>
    properties = {
        "user": "airflow",  # Your PostgreSQL username
        "password": "airflow",  # Your PostgreSQL password
        "driver": "org.postgresql.Driver"  # Driver is specified here for clarity, but Spark handles this.
    }

    # Write the transformed DataFrame to PostgreSQL
    previous_df.write.jdbc(url=jdbc_url, table="player_history", mode="overwrite", properties=properties)

if __name__ == "__main__":
    transform_previous_season()
