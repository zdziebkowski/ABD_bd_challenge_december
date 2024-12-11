from typing import Any, Dict, Optional
import requests
from config import CITIES


def get_city_coordinates(city_name: str) -> tuple[float, float]:
    """
    Retrieve coordinates for a given city name.

    :param city_name: Name of the city.
    :return: Tuple containing latitude and longitude.
    """
    return CITIES[city_name]


def fetch_current_weather(
        latitude: Optional[float],
        longitude: Optional[float],
        city_name: Optional[str]) -> Dict[str, Any]:
    """
    Fetch current weather data from Open-Meteo API.
    The function can be called by specifying either:
    - latitude and longitude directly, or
    - a city_name present in the config.CITIES dictionary.

    :param latitude: Latitude of the desired location (if city_name not provided).
    :param longitude: Longitude of the desired location (if city_name not provided).
    :param city_name: Name of the city for which to fetch weather. If given, overrides latitude/longitude.
    :return: Dictionary with current weather data.
    :raises ValueError: If neither coordinates nor city_name are provided.
    :raises KeyError: If city_name is provided but not found in CITIES.
    :raises requests.HTTPError: If the request to the API fails.
    """
    if city_name is not None:
        latitude, longitude = get_city_coordinates(city_name)
    elif latitude is None or longitude is None:
        raise ValueError("Either city_name or both latitude and longitude must be provided.")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": True
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()
