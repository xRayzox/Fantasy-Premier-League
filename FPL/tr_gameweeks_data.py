from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def transform_gameweeks_data():
     # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FPL Previous Season Data Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "ZAritWM0fgdiJNpV9VIj") \
        .config("spark.hadoop.fs.s3a.secret.key", "FBaOdH4ICewnDOZoLo0vGr39YUQMjkv4CdVIyjEA") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    # Define schema
    schema = StructType([
        StructField('id', StringType(), True),
        StructField('name', StringType(), True),
        StructField('deadline_time', StringType(), True),
        StructField('highest_score', IntegerType(), True),
        StructField('average_entry_score', IntegerType(), True),
        StructField('most_selected', StringType(), True),
        StructField('most_transferred_in', StringType(), True),
        StructField('top_element', StringType(), True),
        StructField('most_captained', StringType(), True),
        StructField('most_vice_captained', StringType(), True)
    ])

   

    # Load gameweeks data from MinIO
    gameweeks_path = "s3a://fpl/data/gameweeks.csv"  # Adjust the path as needed
    df_spark = spark.read.csv(gameweeks_path, schema=schema, header=True)

    # Select needed columns
    needed_columns = ['id', 'name', 'deadline_time', 'highest_score',
                      'average_entry_score', 'most_selected', 'most_transferred_in',
                      'top_element', 'most_captained', 'most_vice_captained']
    df_spark = df_spark.select(*needed_columns)

    # Transform 'deadline_time' column
    df_spark = df_spark.withColumn('deadline_time', F.to_timestamp(df_spark.deadline_time)) \
                       .withColumnRenamed('name', 'gw_name')

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"  # Adjust host and database
    properties = {
        "user": "airflow",  # Your PostgreSQL username
        "password": "airflow",  # Your PostgreSQL password
        "driver": "org.postgresql.Driver"
    }

    # Write the DataFrame to PostgreSQL
    df_spark.write.jdbc(url=jdbc_url, table="fixtures", mode="overwrite", properties=properties)

if __name__ == "__main__":
    transform_gameweeks_data()
