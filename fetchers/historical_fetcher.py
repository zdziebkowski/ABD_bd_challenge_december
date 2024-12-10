from typing import Any, Dict
import requests

def fetch_historical_weather(latitude: float, longitude: float, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Fetch historical weather data from Open-Meteo API based on given latitude, longitude and date range.

    :param latitude: Latitude of the desired location.
    :param longitude: Longitude of the desired location.
    :param start_date: Start date in format YYYY-MM-DD.
    :param end_date: End date in format YYYY-MM-DD.
    :return: Dictionary with the historical weather data.
    :raises requests.HTTPError: If the request fails.
    """
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
