import os
import logging
import time
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


def cleanup_old_backups(keep_latest=5):
    """Keep only specified number of most recent backups"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get all backup tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%_backup_%'
        ORDER BY table_name DESC
    """)

    backup_tables = {}
    for (table_name,) in cur.fetchall():
        base_name = table_name.split('_backup_')[0]
        if base_name not in backup_tables:
            backup_tables[base_name] = []
        backup_tables[base_name].append(table_name)

    # Drop old backups
    for base_name, backups in backup_tables.items():
        if len(backups) > keep_latest:
            for old_backup in sorted(backups)[:-keep_latest]:
                cur.execute(f"DROP TABLE IF EXISTS {old_backup}")

    conn.commit()
    cur.close()
    conn.close()


def backup_tables():
    """Create backup of existing tables before update"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    tables = ['temperature_rankings', 'wind_analysis', 'day_night_patterns', 'weather_codes']
    for table in tables:
        backup_table = f"{table}_backup_{timestamp}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {backup_table} AS SELECT * FROM {table}")

    conn.commit()
    cur.close()
    conn.close()


def create_tables():
    """Create tables for weather data with version tracking"""
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


def load_data_to_postgres_batch(spark, parquet_dir, batch_size=10000):
    """Load Parquet data into PostgreSQL using batching with progress tracking"""
    logger = logging.getLogger(__name__)
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

    # Properties for PostgreSQL connection
    properties = {
        "user": DB_CONFIG['user'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver",
        "batchsize": str(batch_size)
    }

    for table in ['temperature_rankings', 'wind_analysis', 'day_night_patterns', 'weather_codes']:
        logger.info(f"Processing table: {table}")

        # Read Parquet
        df = spark.read.parquet(f"{parquet_dir}/{table}")
        total_rows = df.count()
        num_partitions = (total_rows + batch_size - 1) // batch_size

        logger.info(f"Total rows: {total_rows}, Number of batches: {num_partitions}")

        # Process in batches using row_number
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        # Add row numbers for batching
        window = Window.orderBy("source_city")
        df_numbered = df.withColumn("row_num", row_number().over(window))

        # Write with progress tracking
        try:
            rows_written = 0
            for batch_num in range(num_partitions):
                start_row = batch_num * batch_size + 1
                end_row = (batch_num + 1) * batch_size

                batch_df = df_numbered.filter(
                    (df_numbered.row_num >= start_row) &
                    (df_numbered.row_num <= end_row)
                ).drop("row_num")

                batch_count = batch_df.count()

                batch_df.write \
                    .mode("append" if batch_num > 0 else "overwrite") \
                    .jdbc(jdbc_url, table, properties=properties)

                rows_written += batch_count
                progress = (rows_written / total_rows) * 100
                logger.info(f"Progress for {table}: {progress:.2f}% ({rows_written}/{total_rows} rows)")

        except Exception as e:
            logger.error(f"Error processing batch {batch_num} for table {table}: {str(e)}")
            raise

        logger.info(f"Completed loading table: {table}")


def load_with_retry(func, max_retries=3):
    """Decorator for retry logic"""

    def wrapper(*args, **kwargs):
        logger = logging.getLogger(__name__)
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed after {max_retries} attempts")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Attempt {attempt + 1} failed. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

    return wrapper


@load_with_retry
def load_data(spark, parquet_dir):
    """Load data with retry mechanism"""
    load_data_to_postgres_batch(spark, parquet_dir)


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(current_dir, "logs")

    logger = setup_logging(logs_dir)
    logger.info("Starting PostgreSQL setup and data loading process")

    try:
        # Backup existing tables
        logger.info("Creating backup of existing tables")
        backup_tables()

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
        load_data(spark, parquet_dir)

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
