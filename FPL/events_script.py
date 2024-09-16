import pandas as pd
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transform Events Data") \
    .getOrCreate()

# Assume `data` is the JSON string input
data = '''
[
    {
        "id": 1,
        "name": "Event 1",
        "deadline_time": "2023-10-15T14:00:00Z",
        "average_entry_score": 50,
        "finished": true,
        "data_checked": false,
        "highest_scoring_entry": 300,
        "deadline_time_epoch": 1697378400,
        "highest_score": 100,
        "is_previous": false,
        "is_current": true,
        "is_next": true,
        "cup_leagues_created": true,
        "h2h_ko_matches_created": false,
        "ranked_count": 1500,
        "chip_plays": [{"chip_name": "Wildcard", "num_played": 10}],
        "most_selected": 5,
        "most_transferred_in": 20,
        "top_element": 2,
        "top_element_info": {"id": 2, "points": 50},
        "transfers_made": 5,
        "most_captained": 3,
        "most_vice_captained": 2
    }
]
'''

# Function to process events data
def transform_events_data(json_str):
    # Convert JSON string to Pandas DataFrame using StringIO
    json_io = StringIO(json_str)
    events_df = pd.read_json(json_io, orient='records')

    # Log the raw DataFrame shape and head
    logger.info("Raw DataFrame shape: %s", events_df.shape)
    logger.info("Raw DataFrame head:\n%s", events_df.head())

    # Process `top_element_info`
    def process_top_element_info(x):
        if isinstance(x, dict):
            return [{'id': x.get('id', 0), 'points': x.get('points', 0)}]
        elif x is None:  # Handle None explicitly
            return []
        else:
            logger.warning("Unexpected type for top_element_info: %s", type(x))
            return []

    events_df['top_element_info'] = events_df['top_element_info'].apply(process_top_element_info)

    # Process `chip_plays`
    def process_chip_plays(x):
        if isinstance(x, list):
            return [{'chip_name': chip.get('chip_name', ''), 'num_played': chip.get('num_played', 0)} 
                    for chip in x if isinstance(chip, dict)]
        elif x is None:  # Handle None explicitly
            return []
        else:
            logger.warning("Unexpected type for chip_plays: %s", type(x))
            return []

    events_df['chip_plays'] = events_df['chip_plays'].apply(process_chip_plays)

    # Convert boolean fields to strings
    boolean_fields = [
        'finished', 'data_checked', 'is_previous', 'is_current',
        'is_next', 'cup_leagues_created', 'h2h_ko_matches_created'
    ]
    for field in boolean_fields:
        events_df[field] = events_df[field].apply(
            lambda x: str(x).lower() if isinstance(x, bool) else 'none'
        )
    
    # Replace NaN, None, or null with 0 and ensure integers for numeric fields
    numeric_fields = [
        'average_entry_score', 'highest_scoring_entry', 'highest_score',
        'ranked_count', 'most_selected', 'most_transferred_in',
        'top_element', 'transfers_made', 'most_captained', 'most_vice_captained'
    ]

    # Log the entire DataFrame before creating Spark DataFrame
    logger.info("Pandas DataFrame before Spark conversion:\n%s", events_df)

    # Log the columns and their dtypes
    logger.info("DataFrame columns and their types:\n%s", events_df.dtypes)

    for field in numeric_fields:
        events_df[field] = events_df[field].apply(
            lambda x: 0 if pd.isna(x) or x is None else int(x)
        )

    # Define the schema for the Spark DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("deadline_time", StringType(), True),
        StructField("average_entry_score", IntegerType(), True),
        StructField("finished", StringType(), True),
        StructField("data_checked", StringType(), True),
        StructField("highest_scoring_entry", IntegerType(), True),
        StructField("deadline_time_epoch", IntegerType(), True),
        StructField("highest_score", IntegerType(), True),
        StructField("is_previous", StringType(), True),
        StructField("is_current", StringType(), True),
        StructField("is_next", StringType(), True),
        StructField("cup_leagues_created", StringType(), True),
        StructField("h2h_ko_matches_created", StringType(), True),
        StructField("ranked_count", IntegerType(), True),
        StructField("chip_plays", ArrayType(StructType([
            StructField("chip_name", StringType(), True),
            StructField("num_played", IntegerType(), True)
        ])), True),
        StructField("most_selected", IntegerType(), True),
        StructField("most_transferred_in", IntegerType(), True),
        StructField("top_element", IntegerType(), True),
        StructField("top_element_info", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("points", IntegerType(), True)
        ])), True),
        StructField("transfers_made", IntegerType(), True),
        StructField("most_captained", IntegerType(), True),
        StructField("most_vice_captained", IntegerType(), True)
    ])
    
    # Convert Pandas DataFrame to Spark DataFrame with defined schema
    events_spark_df = spark.createDataFrame(events_df, schema=schema)
    
    return events_spark_df

# Run the transformation function
events_spark_df = transform_events_data(data)

# Stop Spark session
spark.stop()