from typing import Dict, Any, Optional
import typer
from fetchers.current_fetcher import fetch_current_weather, fetch_weather_for_all_cities
from fetchers.historical_fetcher import fetch_historical_weather, fetch_historical_weather_for_all_cities
from utils.file_manager import save_data_to_file

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
    try:
        current_weather: Dict[str, Any] = fetch_current_weather(
            city_name=city,
            latitude=latitude,
            longitude=longitude
        )
        save_data_to_file(current_weather, output)
        if city:
            typer.echo(f"Current weather for {city} saved successfully to {output}.")
        else:
            typer.echo(f"Current weather for coordinates ({latitude}, {longitude}) saved successfully to {output}.")
    except (ValueError, KeyError, requests.HTTPError) as e:
        typer.echo(f"Error fetching current weather: {e}", err=True)


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
    try:
        historical_weather: Dict[str, Any] = fetch_historical_weather(
            city_name=city,
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date
        )
        save_data_to_file(historical_weather, output)
        if city:
            typer.echo(f"Historical weather for {city} from {start_date} to {end_date} saved successfully to {output}.")
        else:
            typer.echo(
                f"Historical weather for coordinates ({latitude}, {longitude}) from {start_date} to {end_date} saved successfully to {output}.")
    except (ValueError, KeyError, requests.HTTPError) as e:
        typer.echo(f"Error fetching historical weather: {e}", err=True)


@app.command()
def fetch_current_all(
        output: str = typer.Option(
            "data/all_current_weather.json",
            help="Output file path for all cities' current weather data."
        )
) -> None:
    """
    Fetch current weather data for all cities in the configuration and save to a JSON file.
    """
    try:
        all_current_weather: Dict[str, Dict[str, Any]] = fetch_weather_for_all_cities()
        save_data_to_file(all_current_weather, output)
        typer.echo(f"Current weather for all cities saved successfully to {output}.")
    except (ValueError, KeyError, requests.HTTPError) as e:
        typer.echo(f"Error fetching current weather for all cities: {e}", err=True)


@app.command()
def fetch_historical_all(
        start_date: str = typer.Option(
            ..., help="Start date for historical data in YYYY-MM-DD format."
        ),
        end_date: str = typer.Option(
            ..., help="End date for historical data in YYYY-MM-DD format."
        ),
        output: str = typer.Option(
            "data/all_historical_weather.json",
            help="Output file path for all cities' historical weather data."
        )
) -> None:
    """
    Fetch historical weather data for all cities in the configuration and save to a JSON file.
    """
    try:
        all_historical_weather: Dict[str, Dict[str, Any]] = fetch_historical_weather_for_all_cities(
            start_date=start_date,
            end_date=end_date
        )
        save_data_to_file(all_historical_weather, output)
        typer.echo(f"Historical weather for all cities from {start_date} to {end_date} saved successfully to {output}.")
    except (ValueError, KeyError, requests.HTTPError) as e:
        typer.echo(f"Error fetching historical weather for all cities: {e}", err=True)


if __name__ == "__main__":
    app()
