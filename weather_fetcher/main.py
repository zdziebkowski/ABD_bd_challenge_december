from typing import Dict, Any, Optional
import typer
import requests
import logging
import os
from datetime import datetime
from weather_fetcher.fetchers.current_fetcher import fetch_current_weather, fetch_weather_for_all_cities
from weather_fetcher.fetchers.historical_fetcher import fetch_historical_weather, fetch_historical_weather_for_all_cities
from weather_fetcher.utils.file_manager import save_data_to_file, save_city_weather_data

# Configure logging
log_directory = os.path.join("weather_fetcher", "logs")
os.makedirs(log_directory, exist_ok=True)
log_file = os.path.join(log_directory, f"weather_fetch_{datetime.now().strftime('%Y%m%d_%H%M')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def fetch_current(
        city: Optional[str] = typer.Option(
            None, help="Name of the city to fetch current weather for."
        ),
        latitude: Optional[float] = typer.Option(
            None, help="Latitude of the location."
        ),
        longitude: Optional[float] = typer.Option(
            None, help="Longitude of the location."
        ),
        output: str = typer.Option(
            "data/current_weather.json", help="Output file path for current weather data."
        )
) -> None:
    """
    Fetch current weather data for a specified city or coordinates and save to a JSON file.
    """
    logger.info("Starting current weather fetch operation")
    location_info = f"city={city}" if city else f"coordinates=({latitude}, {longitude})"
    logger.info(f"Parameters: {location_info}, output={output}")

    try:
        os.makedirs(os.path.dirname(output), exist_ok=True)

        logger.debug("Calling fetch_current_weather function")
        current_weather: Dict[str, Any] = fetch_current_weather(
            city_name=city,
            latitude=latitude,
            longitude=longitude
        )

        logger.debug(f"Saving weather data to {output}")
        save_data_to_file(current_weather, output)

        success_message = f"Current weather for {location_info} saved successfully to {output}"
        logger.info(success_message)
        typer.echo(success_message)

    except (ValueError, KeyError) as e:
        error_message = f"Data processing error for {location_info}: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except requests.HTTPError as e:
        error_message = f"HTTP error occurred while fetching data for {location_info}: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except Exception as e:
        error_message = f"Unexpected error occurred for {location_info}: {str(e)}"
        logger.error(error_message, exc_info=True)
        typer.echo(error_message, err=True)


@app.command()
def fetch_historical(
        city: Optional[str] = typer.Option(
            None, help="Name of the city to fetch historical weather for."
        ),
        latitude: Optional[float] = typer.Option(
            None, help="Latitude of the location."
        ),
        longitude: Optional[float] = typer.Option(
            None, help="Longitude of the location."
        ),
        start_date: str = typer.Option(
            ..., help="Start date for historical data in YYYY-MM-DD format."
        ),
        end_date: str = typer.Option(
            ..., help="End date for historical data in YYYY-MM-DD format."
        ),
        output: str = typer.Option(
            "data/historical_weather.json", help="Output file path for historical weather data."
        )
) -> None:
    """
    Fetch historical weather data for a specified city or coordinates and save to a JSON file.
    """
    logger.info("Starting historical weather fetch operation")
    location_info = f"city={city}" if city else f"coordinates=({latitude}, {longitude})"
    logger.info(f"Parameters: {location_info}, start_date={start_date}, end_date={end_date}, output={output}")

    try:
        os.makedirs(os.path.dirname(output), exist_ok=True)

        logger.debug("Calling fetch_historical_weather function")
        historical_weather: Dict[str, Any] = fetch_historical_weather(
            city_name=city,
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date
        )

        logger.debug(f"Saving historical weather data to {output}")
        save_data_to_file(historical_weather, output)

        success_message = f"Historical weather for {location_info} from {start_date} to {end_date} saved successfully to {output}"
        logger.info(success_message)
        typer.echo(success_message)

    except (ValueError, KeyError) as e:
        error_message = f"Data processing error for {location_info}: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except requests.HTTPError as e:
        error_message = f"HTTP error occurred while fetching data for {location_info}: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except Exception as e:
        error_message = f"Unexpected error occurred for {location_info}: {str(e)}"
        logger.error(error_message, exc_info=True)
        typer.echo(error_message, err=True)


@app.command()
def fetch_current_all(
        output_dir: str = typer.Option(
            "data", help="Directory for saving weather data files."
        )
) -> None:
    """
    Fetch current weather data for all cities in the configuration and save to separate JSON files.
    """
    logger.info(f"Starting current weather fetch for all cities")
    logger.info(f"Output directory: {output_dir}")

    try:
        os.makedirs(output_dir, exist_ok=True)

        logger.debug("Calling fetch_weather_for_all_cities function")
        all_current_weather = fetch_weather_for_all_cities()

        logger.info(f"Processing weather data for {len(all_current_weather)} cities")
        for city, weather_data in all_current_weather.items():
            logger.debug(f"Processing data for city: {city}")
            file_path = save_city_weather_data(city, weather_data, output_dir)
            success_message = f"Current weather for {city} saved successfully to {file_path}"
            logger.info(success_message)
            typer.echo(success_message)

        logger.info("Completed fetching current weather for all cities")

    except (ValueError, KeyError) as e:
        error_message = f"Data processing error while fetching all cities: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except requests.HTTPError as e:
        error_message = f"HTTP error occurred while fetching all cities: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while fetching all cities: {str(e)}"
        logger.error(error_message, exc_info=True)
        typer.echo(error_message, err=True)


@app.command()
def fetch_historical_all(
        start_date: str = typer.Option(
            ..., help="Start date for historical data in YYYY-MM-DD format."
        ),
        end_date: str = typer.Option(
            ..., help="End date for historical data in YYYY-MM-DD format."
        ),
        output_dir: str = typer.Option(
            "data", help="Directory for saving weather data files."
        )
) -> None:
    """
    Fetch historical weather data for all cities in the configuration and save to separate JSON files.
    """
    logger.info("Starting historical weather fetch for all cities")
    logger.info(f"Parameters: start_date={start_date}, end_date={end_date}, output_dir={output_dir}")

    try:
        os.makedirs(output_dir, exist_ok=True)

        logger.debug("Calling fetch_historical_weather_for_all_cities function")
        all_historical_weather = fetch_historical_weather_for_all_cities(
            start_date=start_date,
            end_date=end_date
        )

        logger.info(f"Processing historical weather data for {len(all_historical_weather)} cities")
        for city, weather_data in all_historical_weather.items():
            logger.debug(f"Processing historical data for city: {city}")
            file_path = save_city_weather_data(
                city,
                weather_data,
                output_dir
            )
            success_message = f"Historical weather for {city} from {start_date} to {end_date} saved to {file_path}"
            logger.info(success_message)
            typer.echo(success_message)

        logger.info("Completed fetching historical weather for all cities")

    except (ValueError, KeyError) as e:
        error_message = f"Data processing error while fetching historical data for all cities: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except requests.HTTPError as e:
        error_message = f"HTTP error occurred while fetching historical data for all cities: {str(e)}"
        logger.error(error_message)
        typer.echo(error_message, err=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while fetching historical data for all cities: {str(e)}"
        logger.error(error_message, exc_info=True)
        typer.echo(error_message, err=True)


if __name__ == "__main__":
    logger.info("Weather fetch script started")
    app()
    logger.info("Weather fetch script completed")