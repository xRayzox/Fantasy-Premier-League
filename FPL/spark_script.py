from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

def main():
    # Create Spark session
    spark = SparkSession.builder.appName("KafkaToSpark").getOrCreate()

    # Define the schema
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

    # Read data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "FPL_Teams") \
        .load()

    # Transform data using schema
    df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Select specific columns for display
    df = df.select("name", "short_name", "strength_overall_home", "strength_overall_away")

    # Write the stream to console with customized display
    query = df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
