import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def setup_logging(logs_dir):
    """Setup basic logging to both file and console"""
    os.makedirs(logs_dir, exist_ok=True)

    # Create log filename with timestamp
    log_file = os.path.join(logs_dir, f"json_to_parquet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def create_spark_session():
    """Create a basic Spark session"""
    return (SparkSession.builder
            .appName("JSON_to_Parquet_Converter")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())


def process_weather_data(spark, input_dir, logger):
    """Read and process JSON weather files"""
    logger.info(f"Processing weather data from: {input_dir}")

    # Read JSON files
    json_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir)
                  if f.endswith('.json')]
    if not json_files:
        logger.error(f"No JSON files found in {input_dir}")
        raise FileNotFoundError(f"No JSON files found in {input_dir}")

    logger.info(f"Found {len(json_files)} JSON files")

    # Read and flatten data
    df = spark.read.option("multiLine", True).json(json_files)
    logger.info("Successfully read JSON files")

    flattened_df = df.select(
        regexp_extract(input_file_name(), r'([^/]+?)_\d{8}', 1).alias("source_city"),
        "latitude",
        "longitude",
        "elevation",
        "current_weather.time",
        current_timestamp().alias("processing_timestamp"),
        "current_weather.temperature",
        "current_weather.windspeed",
        "current_weather.winddirection",
        "current_weather.is_day",
        "current_weather.weathercode"
    )
    logger.info("Successfully flattened data")

    return flattened_df


def show_data_preview(df, logger):
    """Display data preview and statistics"""
    logger.info("Generating data preview")
    row_count = df.count()
    logger.info(f"Total rows: {row_count}")

    print("\nSchema:")
    df.printSchema()
    print("\nSample Data:")
    df.show(5, truncate=False)


def save_to_parquet(df, output_dir, logger):
    """Save DataFrame as Parquet file"""
    logger.info(f"Saving data to Parquet: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    try:
        df.coalesce(1).write.mode("overwrite").parquet(output_dir)
        logger.info("Successfully saved Parquet file")
    except Exception as e:
        logger.error(f"Error saving Parquet file: {str(e)}")
        raise


def main():
    # Setup paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(current_dir, "data", "current")
    output_dir = os.path.join(current_dir, "parquet")
    logs_dir = os.path.join(current_dir, "logs")

    # Setup logging
    logger = setup_logging(logs_dir)
    logger.info("Starting weather data ETL process")

    # Process data
    spark = None
    try:
        spark = create_spark_session()
        logger.info("Created Spark session")

        # Process
        weather_df = process_weather_data(spark, input_dir, logger)

        # Preview
        show_data_preview(weather_df, logger)

        # Save
        save_to_parquet(weather_df, output_dir, logger)

        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
