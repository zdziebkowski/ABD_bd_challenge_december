from typing import Any, Dict
import json
import os
from datetime import datetime


def get_formatted_filename(city: str, base_dir: str = "data") -> str:
    """
    Generate a formatted filename for weather data.

    :param city: Name of the city
    :param base_dir: Base directory for saving files
    :return: Formatted file path string
    """
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(base_dir, f"{city}_{current_time}.json")


def save_data_to_file(data: Dict[str, Any], file_path: str) -> None:
    """
    Save given data dictionary as JSON file to the specified file path.

    :param data: Dictionary to save.
    :param file_path: File path (directories will be created if not present).
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def save_city_weather_data(city: str, data: Dict[str, Any], base_dir: str = "data") -> str:
    """
    Save weather data for a specific city with formatted filename.

    :param city: Name of the city
    :param data: Weather data to save
    :param base_dir: Base directory for saving files
    :return: Path where the file was saved
    """
    file_path = get_formatted_filename(city, base_dir)
    save_data_to_file(data, file_path)
    return file_path
