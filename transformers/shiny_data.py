from utils.database_connection import load_data


def save_data_to_csv():
    try:
        data = load_data()

        data['temperature_ranking'].to_csv("temperature_ranking.csv", index=False)
        data['weather_codes'].to_csv("weather_codes.csv", index=False)
        data['map_data'].to_csv("map_data.csv", index=False)

        print("Dane zapisane poprawnie do plików CSV.")
    except Exception as e:
        print(f"Wystąpił błąd podczas zapisywania danych: {e}")


if __name__ == "__main__":
    save_data_to_csv()
