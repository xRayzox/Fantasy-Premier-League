from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import pyspark.sql.functions as F

def transform_players():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FPL Players Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "JgaaxXsjCxGVJBbNKmup") \
        .config("spark.hadoop.fs.s3a.secret.key", "sQWHycmBpXlcoxDKluOMuc66LKJ756UqBDa7ofE7") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Load player data from MinIO
    players_path = "s3a://fpl/data/players.csv"
    players_df = spark.read.csv(players_path, header=True, inferSchema=True)

    # Select only the necessary columns
    needed_columns = [
        'code', 'id', 'first_name', 'second_name', 
        'web_name', 'element_type', 'team', 'team_code', 
        'dreamteam_count', 'news', 'value_season'
    ]
    
    df = players_df.select(*needed_columns)

    # Replace empty news entries
    df = df.withColumn('news', F.when(F.col('news') == '', 'No news').otherwise(F.col('news')))
    
    # Create full_name column
    df = df.withColumn('full_name', F.concat(F.col('first_name'), F.lit(' '), F.col('second_name')))
    
    # Drop first_name and second_name columns
    df = df.drop('first_name', 'second_name')

    # Define the schema for players
    schema = StructType([
        StructField("code", StringType(), True),
        StructField("id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("web_name", StringType(), True),
        StructField("element_type", StringType(), True),
        StructField("team", StringType(), True),
        StructField("team_code", StringType(), True),
        StructField("dreamteam_count", IntegerType(), True),
        StructField("news", StringType(), True),
        StructField("value_season", FloatType(), True)
    ])

    # Rename the columns in df to match the schema (optional)
    df = df.select(
        "code", "id", "full_name", "web_name", 
        "element_type", "team", "team_code", 
        "dreamteam_count", "news", "value_season"
    )

    # Write the transformed DataFrame back to PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(url=jdbc_url, table="players", mode="overwrite", properties=properties)

if __name__ == "__main__":
    transform_players()
