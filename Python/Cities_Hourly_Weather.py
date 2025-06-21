import requests
import time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import os
from pymongo import MongoClient
print("All necessary packages have been installed sucessfully")

# Your OpenWeatherMap API Key (set as env variable)
API_KEY = os.getenv("OPENWEATHER_API_KEY")

#API Endpoints
GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
ONECALL_URL = "https://api.openweathermap.org/data/2.5/onecall"
AIR_QUALITY_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

# Connecting to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["weather_etl"]
collection = db["hourly_weather_logs"]
print('successfully connect to the ', db)

# Scrape top 200 cities and countries
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
        if len(cols) >= 4:
            city = cols[2].text.strip()
            country = cols[3].text.strip()
            data.append([city, country])

    df = pd.DataFrame(data, columns=["City", "Country"])
    print(df.head(5))
    return np.array(df["City"]), np.array(df["Country"])

# Get coordinates from city name
def geocode_city(city, country):
    params = {"q": f"{city},{country}", "limit": 1, "appid": API_KEY}
    r = requests.get(GEOCODE_URL, params=params)
    if r.status_code == 200 and r.json():
        data = r.json()[0]
        return data["lat"], data["lon"]
    return None, None

# Get hourly weather
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

# Get air quality data
def fetch_air_quality(lat, lon):
    params = {"lat": lat, "lon": lon, "appid": API_KEY}
    r = requests.get(AIR_QUALITY_URL, params=params)
    if r.status_code == 200:
        data = r.json().get("list", [{}])[0]
        aqi = data.get("main", {}).get("aqi", "N/A")
        components = data.get("components", {})
        return {
            "aqi": aqi,
            "pm2_5": components.get("pm2_5", "N/A"),
            "pm10": components.get("pm10", "N/A"),
            "co": components.get("co", "N/A"),
            "no2": components.get("no2", "N/A")
        }
    return {"aqi": "N/A", "pm2_5": "N/A", "pm10": "N/A", "co": "N/A", "no2": "N/A"}

# One full data run
def run_once(cities_np, countries_np, run_id):
    documents = []

    for i in range(len(cities_np)):
        city = cities_np[i]
        country = countries_np[i]
        print(f"[Run {run_id}] {i+1}/{len(cities_np)}: {city}, {country}")

        lat, lon = geocode_city(city, country)
        if lat is None:
            print(f"Could not geocode {city}, {country}")
            continue

        hourly_data = fetch_hourly_weather(lat, lon)
        if not hourly_data:
            print(f"No weather data for {city}")
            continue

        hour = hourly_data[0]
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(hour["dt"]))
        temp = hour.get("temp", "N/A")
        humidity = hour.get("humidity", "N/A")
        rain = hour.get("rain", {}).get("1h", 0)
        snow = hour.get("snow", {}).get("1h", 0)
        precipitation = rain + snow

        air_quality = fetch_air_quality(lat, lon)

        documents.append({
            "run_id": run_id,
            "city": city,
            "country": country,
            "timestamp_utc": dt,
            "weather": {
                "temperature_c": temp,
                "humidity": humidity,
                "precipitation_mm": precipitation
            },
            "air_quality": air_quality
        })

        time.sleep(0.5)  #Avoid hitting rate limits

    if documents:
        collection.insert_many(documents)
        print(f"Inserted {len(documents)} records to MongoDB.")
    else:
        print("No documents to insert.")

#Main execution loop
def main():
    cities_np, countries_np = fetch_top_200_cities()
    print("Loaded top 200 cities.")
    run_id = 1

    while True:
        print(f"\n Starting run #{run_id} at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        run_once(cities_np, countries_np, run_id)
        print(f"Run {run_id} completed and logged.")
        run_id += 1
        print("Sleeping for 1 hour...\n")
        time.sleep(3600)

if __name__ == "__main__":
    main()
