import requests
import time
import csv
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import os

# 🔐 Your OpenWeatherMap API Key
API_KEY = "YOUR_OPENWEATHERMAP_API_KEY"
GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
ONECALL_URL = "https://api.openweathermap.org/data/2.5/onecall"

CSV_FILE = "weather_data_log.csv"

def fetch_top_200_cities():
    url = "https://worldpopulationreview.com/cities"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table")
    rows = table.find("tbody").find_all("tr")

    data = []
    for row in rows[:200]:
        cols = row.find_all("td")
        if len(cols) >= 3:
            city = cols[1].text.strip()
            country = cols[2].text.strip()
            data.append([city, country])

    df = pd.DataFrame(data, columns=["City", "Country"])
    return np.array(df["City"]), np.array(df["Country"])

def geocode_city(city, country):
    params = {"q": f"{city},{country}", "limit": 1, "appid": API_KEY}
    r = requests.get(GEOCODE_URL, params=params)
    if r.status_code == 200 and r.json():
        data = r.json()[0]
        return data["lat"], data["lon"]
    return None, None

def fetch_hourly_weather(lat, lon):
    params = {
        "lat": lat, "lon": lon,
        "exclude": "current,minutely,daily,alerts",
        "units": "metric",
        "appid": API_KEY
    }
    r = requests.get(ONECALL_URL, params=params)
    if r.status_code == 200:
        return r.json().get("hourly", [])
    elif r.status_code == 429:
        print("Rate limited. Sleeping for 10 seconds...")
        time.sleep(10)
        return fetch_hourly_weather(lat, lon)
    return []

def run_once(cities_np, countries_np, run_id):
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Run ID", "City", "Country", "Datetime UTC", "Temperature (C)", "Humidity (%)", "Precipitation (mm)"])

        for i in range(len(cities_np)):
            city = cities_np[i]
            country = countries_np[i]
            print(f"[Run {run_id}] 🌍 {i+1}/{len(cities_np)}: {city}, {country}")

            lat, lon = geocode_city(city, country)
            if lat is None:
                continue

            hourly_data = fetch_hourly_weather(lat, lon)
            if not hourly_data:
                continue

            # Record only current hour (hour[0])
            hour = hourly_data[0]
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(hour["dt"]))
            temp = hour.get("temp", "N/A")
            humidity = hour.get("humidity", "N/A")
            rain = hour.get("rain", {}).get("1h", 0)
            snow = hour.get("snow", {}).get("1h", 0)
            precipitation = rain + snow

            writer.writerow([run_id, city, country, dt, temp, humidity, precipitation])

            time.sleep(0.5)  # Respectful delay

def main():
    cities_np, countries_np = fetch_top_200_cities()
    print("📍 Loaded top 200 cities.")
    run_id = 1

    while True:
        print(f"\n⏳ Starting run #{run_id} at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        run_once(cities_np, countries_np, run_id)
        print(f"✅ Run {run_id} completed and logged.")
        run_id += 1
        print("💤 Sleeping for 1 hour...\n")
        time.sleep(3600)  # Wait one hour

if __name__ == "__main__":
    main()
