from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def transform_teams():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FPL Teams Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "PUAF7tOevViDzWE2oR5C") \
        .config("spark.hadoop.fs.s3a.secret.key", "Ui9Ro0scL8MJgGVgzy1QAo3RXXYHj2ms2SQ4UmRi") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Load team data from MinIO
    teams_path = "s3a://fpl/data/teams.csv"
    teams_df = spark.read.csv(teams_path, header=True, inferSchema=True)

    # Select only the necessary columns
    needed_columns = [
        'code', 'id', 'name', 'short_name', 'strength',
        'strength_overall_away', 'strength_overall_home',
        'strength_attack_away', 'strength_attack_home',
        'strength_defence_away', 'strength_defence_home'
    ]
    
    # Select the needed columns
    teams_df = teams_df.select(*needed_columns)

    # Rename the 'name' column to 'team_name'
    teams_df = teams_df.withColumnRenamed('name', 'team_name')

    # Define the schema for teams
    schema = StructType([
        StructField("code", StringType(), True),
        StructField("id", StringType(), True),
        StructField("team_name", StringType(), True),  # Updated to use 'team_name'
        StructField("short_name", StringType(), True),
        StructField("strength", IntegerType(), True),
        StructField("strength_overall_away", IntegerType(), True),
        StructField("strength_overall_home", IntegerType(), True),
        StructField("strength_attack_away", IntegerType(), True),
        StructField("strength_attack_home", IntegerType(), True),
        StructField("strength_defence_away", IntegerType(), True),
        StructField("strength_defence_home", IntegerType(), True)
    ])

    # Create a DataFrame with the defined schema (not needed anymore)
    # teams_spark_df = spark.createDataFrame(df.rdd, schema=schema)  # Remove this line

    # Write the transformed DataFrame back to PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    teams_df.write.jdbc(url=jdbc_url, table="teams", mode="overwrite", properties=properties)

if __name__ == "__main__":
    transform_teams()
