from typing import Dict, Any

from fetchers.current_fetcher import fetch_current_weather
from fetchers.historical_fetcher import fetch_historical_weather
from utils.file_manager import save_data_to_file


def main() -> None:
    # Przykładowe współrzędne Warszawy
    latitude: float = 52.2297
    longitude: float = 21.0122

    # Pobieranie aktualnej pogody
    current_weather: Dict[str, Any] = fetch_current_weather(latitude=latitude, longitude=longitude)
    save_data_to_file(current_weather, "data/current_weather.json")

    # Pobieranie historycznej pogody (np. za 1 stycznia 2023)
    historical_weather: Dict[str, Any] = fetch_historical_weather(
        latitude=latitude,
        longitude=longitude,
        start_date="2023-01-01",
        end_date="2023-01-02"
    )
    save_data_to_file(historical_weather, "data/historical_weather.json")


if __name__ == "__main__":
    main()
