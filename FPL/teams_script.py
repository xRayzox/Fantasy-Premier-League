from pyspark.sql import SparkSession
import sys
import json
import pandas as pd
from io import StringIO
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_teams_data():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("TeamsDataTransformation") \
        .getOrCreate()
    
    try:
        # Read JSON string from the command line argument
        json_str = sys.argv[1]
        
        # Convert JSON string to Pandas DataFrame using StringIO
        json_io = StringIO(json_str)
        teams_df = pd.read_json(json_io, orient='records')
        
        # Check the DataFrame content
        logger.info("Pandas DataFrame head:\n%s", teams_df.head())
        
        # Convert Pandas DataFrame to Spark DataFrame
        teams_spark_df = spark.createDataFrame(teams_df)
        
        # Perform transformations: select only necessary columns
        transformed_df = teams_spark_df \
            .select(
                "id",
                "code",
                "name",
                "short_name",
                "strength",
                "strength_overall_home",
                "strength_overall_away",
                "strength_attack_home",
                "strength_attack_away",
                "strength_defence_home",
                "strength_defence_away",
            )
        logger.info("Transformed Row: %s", transformed_df)   
    except Exception as e:
        logger.error("Error occurred: %s", e)
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    transform_teams_data()
