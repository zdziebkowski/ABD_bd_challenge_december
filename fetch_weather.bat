@echo off
cd /d "G:\Data\projekty\Big Data Challenge"
call .\venv_spark\Scripts\activate
python -m weather_fetcher.main fetch-current-all --output-dir "weather_fetcher/data/current"
pause