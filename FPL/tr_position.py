from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def transform_positions():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FPL Position Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "JgaaxXsjCxGVJBbNKmup") \
        .config("spark.hadoop.fs.s3a.secret.key", "sQWHycmBpXlcoxDKluOMuc66LKJ756UqBDa7ofE7") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Load position data from MinIO
    positions_path = "s3a://fpl/data/positions.csv"
    positions_df = spark.read.csv(positions_path, header=True, inferSchema=True)

    # Select only the necessary columns using select
    positions_df = positions_df.select('id', 'plural_name', 'plural_name_short', 'singular_name', 'singular_name_short')

    # Write the transformed DataFrame back to PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    positions_df.write.jdbc(url=jdbc_url, table="positions", mode="overwrite", properties=properties)

if __name__ == "__main__":
    transform_positions()
