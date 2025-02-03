import pandas as pd
import psycopg2
from utils.db_config import DB_CONFIG


def get_db_connection():
    """Create and return a database connection."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        raise Exception(f"Error connecting to the database: {str(e)}")


def load_data():
    """
    Load all required data from the database.

    Returns:
        Dict containing DataFrames for each required table
    """
    try:
        conn = get_db_connection()

        # Dictionary to store our DataFrames
        dataframes = {
            'map_data': pd.read_sql(
                "SELECT * FROM map_avg_temp ORDER BY source_city",
                conn
            ),
            'temperature_ranking': pd.read_sql(
                "SELECT * FROM ranking_temperature ORDER BY avg_temp DESC",
                conn
            ),
            'weather_codes': pd.read_sql(
                "SELECT * FROM top_weather_code ORDER BY freq DESC",
                conn
            )
        }

        conn.close()
        return dataframes

    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return {}