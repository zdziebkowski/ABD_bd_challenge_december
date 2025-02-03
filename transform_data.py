import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType



def setup_logging(logs_dir: str) -> logging.Logger:
    """
    Set up logging configuration for both file and console output.

    Args:
        logs_dir (str): Directory path where log files will be stored

    Returns:
        logging.Logger: Configured logger instance
    """
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"data_transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark session for data processing.

    Returns:
        SparkSession: Configured Spark session
    """
    logger = logging.getLogger(__name__)
    logger.info("Creating Spark session")
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Weather_Data_Analysis")
        .getOrCreate()
    )


def read_parquet_data(spark: SparkSession, input_dir: str) -> DataFrame:
    """
    Read Parquet files from specified directory.

    Args:
        spark (SparkSession): Active Spark session
        input_dir (str): Directory containing Parquet files

    Returns:
        DataFrame: Spark DataFrame containing Parquet data
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Reading Parquet data from {input_dir}")
    df = spark.read.parquet(input_dir)
    logger.info(f"DataFrame loaded successfully")
    return df


def clean_city_names(df: DataFrame) -> DataFrame:
    """
    Clean city names by replacing '%20' with spaces.

    Args:
        df (DataFrame): Input DataFrame with weather data

    Returns:
        DataFrame: DataFrame with cleaned city names
    """
    logger = logging.getLogger(__name__)
    logger.info("Cleaning city names (replacing '%20' with ' ')")
    return df.withColumn("source_city", regexp_replace("source_city", "%20", " "))


def ranking_temperature(df: DataFrame) -> DataFrame:
    """
    Calculate average temperature per city and return ranking from highest to lowest.

    Args:
        df (DataFrame): Input DataFrame with weather data (columns: source_city, temperature)
    Returns:
        DataFrame: Ranking of cities by avg_temp descending
    """
    logger = logging.getLogger(__name__)
    logger.info("Calculating ranking of cities by average temperature.")

    # Group by city and compute avg temperature
    grouped_df = df.groupBy("source_city").agg(
        round(avg("temperature"), 2).alias("avg_temp")
    )

    # Sort descending
    ranked_df = grouped_df.orderBy(desc("avg_temp"))

    logger.info(f"Ranking temperature: computed for {ranked_df.count()} cities.")
    return ranked_df


def top_weather_code(df: DataFrame) -> DataFrame:
    """
    Find the most frequent weather code for each city, with a text description.

    Args:
        df (DataFrame): Input DataFrame with columns (source_city, weathercode)

    Returns:
        DataFrame: Each row = [source_city, weathercode, freq, weather_description]
                   showing the top (most frequent) code in that city.
    """
    logger = logging.getLogger(__name__)
    logger.info("Calculating top weather code by frequency for each city.")

    # Count occurrences of each (city, code)
    freq_df = df.groupBy("source_city", "weathercode").agg(
        count("*").alias("freq")
    )

    # Rank by frequency within each city
    window_city = Window.partitionBy("source_city").orderBy(desc("freq"))
    ranked_df = freq_df.withColumn("rn", row_number().over(window_city))

    # Keep only top 1 row per city
    top_codes = ranked_df.filter(col("rn") == 1).drop("rn")

    # Add weather description using when/otherwise
    weather_codes = top_codes.withColumn(
        "weather_description",
        when(col("weathercode") == 0, "Clear Sky")
        .when(col("weathercode") == 1, "Mainly Clear")
        .when(col("weathercode") == 2, "Partly Cloudy")
        .when(col("weathercode") == 3, "Overcast")
        .when(col("weathercode") == 45, "Fog")
        .when(col("weathercode") == 48, "Depositing rime fog")
        .when(col("weathercode") == 51, "Drizzle: Light")
        .when(col("weathercode") == 53, "Drizzle: Moderate")
        .when(col("weathercode") == 55, "Drizzle: Dense")
        .otherwise("Unknown")
    )

    logger.info(f"Top weather codes computed for {weather_codes.count()} cities.")
    return weather_codes


def map_avg_temp(df: DataFrame) -> DataFrame:
    """
    Prepare data for a city map: city + lat/long + average temperature.

    Args:
        df (DataFrame): Input data with columns (source_city, latitude, longitude, temperature)

    Returns:
        DataFrame: [source_city, latitude, longitude, avg_temp]
    """
    logger = logging.getLogger(__name__)
    logger.info("Preparing map data: average temperature with lat/long per city.")

    grouped_df = df.groupBy("source_city", "latitude", "longitude").agg(
        round(avg("temperature"), 2).alias("avg_temp")
    )

    logger.info(f"Map data prepared for {grouped_df.count()} rows.")
    return grouped_df


def write_dataframes(dfs_dict: dict, output_dir: str):
    """
    Write DataFrames to Parquet files.

    Args:
        dfs_dict (dict): Dictionary of { 'df_name': df_object }
        output_dir (str): Output directory for Parquet files
    """
    logger = logging.getLogger(__name__)
    for name, df in dfs_dict.items():
        path = f"{output_dir}/{name}"
        logger.info(f"Writing DataFrame '{name}' to Parquet -> {path}")
        df.write.mode("overwrite").parquet(path)


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(current_dir, "parquet")
    output_dir = os.path.join(current_dir, "analysis_results")
    logs_dir = os.path.join(current_dir, "logs")

    logger = setup_logging(logs_dir)
    logger.info("Starting weather data transformation process")

    spark = None
    try:
        spark = create_spark_session()

        df = read_parquet_data(spark, input_dir)

        df = clean_city_names(df)

        temp_rank_df = ranking_temperature(df)
        top_code_df = top_weather_code(df)
        map_df = map_avg_temp(df)

        os.makedirs(output_dir, exist_ok=True)
        dfs_to_write = {
            "ranking_temperature": temp_rank_df,
            "top_weather_code": top_code_df,
            "map_avg_temp": map_df
        }
        write_dataframes(dfs_to_write, output_dir)

        logger.info("Transformation process completed successfully.")

    except Exception as e:
        logger.error(f"Transformation process failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
