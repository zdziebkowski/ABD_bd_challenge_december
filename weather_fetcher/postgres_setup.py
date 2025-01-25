import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from db_config import DB_CONFIG


def setup_logging(logs_dir):
    """Setup logging configuration"""
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"postgres_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def create_database():
    """Create the database if it doesn't exist"""
    conn = psycopg2.connect(
        dbname='postgres',
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host']
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Create database if it doesn't exist
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG['dbname'],))
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {DB_CONFIG['dbname']}")

    cur.close()
    conn.close()


def create_tables():
    """Create tables for weather data"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Temperature Rankings table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS temperature_rankings (
        source_city VARCHAR(100),
        avg_temp NUMERIC(5,2),
        max_temp NUMERIC(5,2),
        min_temp NUMERIC(5,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (source_city)
    )""")

    # Wind Analysis table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wind_analysis (
        source_city VARCHAR(100),
        avg_windspeed NUMERIC(5,2),
        max_windspeed NUMERIC(5,2),
        median_direction INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (source_city)
    )""")

    # Day/Night Patterns table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS day_night_patterns (
        source_city VARCHAR(100),
        is_day BOOLEAN,
        avg_temp NUMERIC(5,2),
        avg_windspeed NUMERIC(5,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (source_city, is_day)
    )""")

    # Weather Codes table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_codes (
        source_city VARCHAR(100),
        weathercode INTEGER,
        frequency INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (source_city, weathercode)
    )""")

    conn.commit()
    cur.close()
    conn.close()


def create_spark_session():
    """Create Spark session with PostgreSQL JDBC driver"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    jdbc_jar = os.path.join(current_dir, "lib", "postgresql-42.2.23.jar")
    return (SparkSession.builder
            .appName("Weather_Data_To_PostgreSQL")
            .config("spark.jars", jdbc_jar)
            .getOrCreate())


def load_data_to_postgres(spark, parquet_dir):
    """Load Parquet data into PostgreSQL tables"""
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

    # Properties for PostgreSQL connection
    properties = {
        "user": DB_CONFIG['user'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }

    # Load and write each dataset
    tables = ['temperature_rankings', 'wind_analysis', 'day_night_patterns', 'weather_codes']

    for table in tables:
        df = spark.read.parquet(f"{parquet_dir}/{table}")

        # Write to PostgreSQL
        df.write \
            .mode("overwrite") \
            .jdbc(jdbc_url, table, properties=properties)


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(current_dir, "logs")

    logger = setup_logging(logs_dir)
    logger.info("Starting PostgreSQL setup and data loading process")

    try:
        # Setup database and tables
        logger.info("Creating database and tables")
        create_database()
        create_tables()

        # Initialize Spark and load data
        logger.info("Initializing Spark session")
        spark = create_spark_session()

        # Get parquet directory path
        parquet_dir = os.path.join(current_dir, "analysis_results")

        logger.info("Loading data to PostgreSQL")
        load_data_to_postgres(spark, parquet_dir)

        logger.info("Data loading completed successfully")

    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
