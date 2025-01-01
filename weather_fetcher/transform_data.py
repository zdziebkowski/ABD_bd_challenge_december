import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *


def setup_logging(logs_dir):
    """Set up logging configuration for both file and console output.

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


def create_spark_session():
    """Create and configure a Spark session for data processing.

    Returns:
        SparkSession: Configured Spark session
    """
    logger = logging.getLogger(__name__)
    logger.info("Creating Spark session")
    return (SparkSession.builder
            .appName("Weather_Data_Analysis")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())


def read_parquet_data(spark, input_dir) -> DataFrame:
    """Read Parquet files from specified directory.

    Args:
        spark (SparkSession): Active Spark session
        input_dir (str): Directory containing Parquet files

    Returns:
        DataFrame: Spark DataFrame containing Parquet data
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Reading Parquet data from {input_dir}")
    return spark.read.parquet(input_dir)


def clean_city_names(df: DataFrame) -> DataFrame:
    """Clean city names by replacing %20 with spaces.

    Args:
        df (DataFrame): Input DataFrame with weather data

    Returns:
        DataFrame: DataFrame with cleaned city names
    """
    logger = logging.getLogger(__name__)
    logger.info("Cleaning city names")
    return df.withColumn("source_city",
                         regexp_replace("source_city", "%20", " "))


def analyze_temperatures(df) -> DataFrame:
    """Calculate temperature statistics grouped by city.

    Args:
        df (DataFrame): Input DataFrame with weather data

    Returns:
        DataFrame: Temperature analysis results
    """
    logger = logging.getLogger(__name__)
    logger.info("Analyzing temperature data")
    return df.groupBy("source_city").agg(
        round(avg("temperature"), 2).alias("avg_temp"),
        round(max("temperature"), 2).alias("max_temp"),
        round(min("temperature"), 2).alias("min_temp")
    ).orderBy(desc("avg_temp"))


def analyze_wind(df) -> DataFrame:
    """Calculate wind statistics grouped by city.

    Args:
        df (DataFrame): Input DataFrame with weather data

    Returns:
        DataFrame: Wind analysis results
    """
    logger = logging.getLogger(__name__)
    logger.info("Analyzing wind data")
    return df.groupBy("source_city").agg(
        round(avg("windspeed"), 2).alias("avg_windspeed"),
        round(max("windspeed"), 2).alias("max_windspeed"),
        expr("percentile_approx(winddirection, 0.5)").alias("median_direction")
    ).orderBy(desc("avg_windspeed"))


def analyze_day_night(df) -> DataFrame:
    """Analyze temperature and wind patterns between day and night.

    Args:
        df (DataFrame): Input DataFrame with weather data

    Returns:
        DataFrame: Day/night analysis results
    """
    logger = logging.getLogger(__name__)
    logger.info("Analyzing day/night patterns")
    return df.groupBy("source_city", "is_day").agg(
        round(avg("temperature"), 2).alias("avg_temp"),
        round(avg("windspeed"), 2).alias("avg_windspeed")
    ).orderBy("source_city", "is_day")


def analyze_weather_codes(df) -> DataFrame:
    """Calculate frequency of weather codes by city.

    Args:
        df (DataFrame): Input DataFrame with weather data

    Returns:
        DataFrame: Weather code frequency analysis
    """
    logger = logging.getLogger(__name__)
    logger.info("Analyzing weather codes")
    return df.groupBy("source_city", "weathercode").agg(
        count("*").alias("frequency")
    ).orderBy("source_city", desc("frequency"))


def write_dataframes(dfs_dict: dict, output_dir: str):
    """Write DataFrames to Parquet files.

    Args:
        dfs_dict (dict): Dictionary of DataFrames with their names
        output_dir (str): Output directory for Parquet files
    """
    logger = logging.getLogger(__name__)
    for name, df in dfs_dict.items():
        logger.info(f"Writing {name} to Parquet")
        df.write.mode("overwrite").parquet(f"{output_dir}/{name}")


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(current_dir, "parquet")
    output_dir = os.path.join(current_dir, "analysis_results")
    logs_dir = os.path.join(current_dir, "logs")

    logger = setup_logging(logs_dir)
    logger.info("Starting weather data transformation process")

    try:
        spark = create_spark_session()
        parquet_df = read_parquet_data(spark, input_dir)
        os.makedirs(output_dir, exist_ok=True)

        # Clean city names
        df = clean_city_names(parquet_df)

        # Analyze data
        temp_df = analyze_temperatures(df)
        wind_df = analyze_wind(df)
        day_night_df = analyze_day_night(df)
        weather_codes_df = analyze_weather_codes(df)

        # Show results
        print("\nTemperature Rankings:")
        temp_df.show(truncate=False)

        print("\nWind Analysis:")
        wind_df.show(truncate=False)

        print("\nDay/Night Patterns:")
        day_night_df.show(truncate=False)

        print("\nWeather Codes:")
        weather_codes_df.show(truncate=False)

        # Write results
        dfs_to_write = {
            "temperature_rankings": temp_df,
            "wind_analysis": wind_df,
            "day_night_patterns": day_night_df,
            "weather_codes": weather_codes_df
        }
        write_dataframes(dfs_to_write, output_dir)

        logger.info("Transformation process completed successfully")

    except Exception as e:
        logger.error(f"Transformation process failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
