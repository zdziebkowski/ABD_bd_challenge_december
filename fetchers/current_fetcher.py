from typing import Any, Dict
import requests

def fetch_current_weather(latitude: float, longitude: float) -> Dict[str, Any]:
    """
   Fetch current weather data from Open-Meteo API based on given latitude and longitude.

   :param latitude: Latitude of the desired location.
   :param longitude: Longitude of the desired location.
   :return: Dictionary with the current weather data.
   :raises requests.HTTPError: If the request fails.
   """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": True
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()