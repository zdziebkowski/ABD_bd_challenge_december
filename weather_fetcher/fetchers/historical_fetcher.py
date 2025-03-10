from typing import Any, Dict, Optional
import requests
from weather_fetcher.fetchers.config import CITIES


def get_city_coordinates(city_name: str) -> tuple[float, float]:
    """
    Retrieve the coordinates (latitude, longitude) for a given city name.
    Raises a KeyError if the city is not found.
    """
    return CITIES[city_name]


def fetch_historical_weather(
        start_date: str,
        end_date: str,
        city_name: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None
) -> Dict[str, Any]:
    """
    Fetch historical weather data from the Open-Meteo ERA5 API.
    Can be done by either providing city_name (from config. CITIES)
    or by specifying latitude and longitude directly.

    :param start_date: Start date in format YYYY-MM-DD (required).
    :param end_date: End date in format YYYY-MM-DD (required).
    :param city_name: Name of the city to fetch weather for (optional).
    :param latitude: Latitude of the location (required if city_name not provided).
    :param longitude: Longitude of the location (required if city_name not provided).
    :return: Dictionary with the historical weather data.
    :raises ValueError: If required parameters are missing.
    :raises KeyError: If city_name is provided but not found in CITIES.
    :raises requests.HTTPError: If the request to the API fails.
    """

    if city_name is not None:
        latitude, longitude = get_city_coordinates(city_name)
    else:
        if latitude is None or longitude is None:
            raise ValueError("You must provide either city_name or both latitude and longitude.")

    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required.")

    url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def get_city_historical_weather(
        city_name: str,
        start_date: str,
        end_date: str
) -> Dict[str, Any]:
    """
    Retrieve historical weather for a specific city by name.

    :param city_name: Name of the city to fetch weather for.
    :param start_date: Start date for historical data.
    :param end_date: End date for historical data.
    :return: Dictionary with historical weather data for the city.
    """
    return fetch_historical_weather(
        city_name=city_name,
        start_date=start_date,
        end_date=end_date
    )


def fetch_historical_weather_for_all_cities(
        start_date: str,
        end_date: str
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch historical weather data for all cities in the CITIES dictionary.

    :param start_date: Start date for historical data.
    :param end_date: End date for historical data.
    :return: Dictionary with city names as keys and their historical weather data as values.
    """
    return {
        city: fetch_historical_weather(
            city_name=city,
            start_date=start_date,
            end_date=end_date
        )
        for city in CITIES.keys()
    }
