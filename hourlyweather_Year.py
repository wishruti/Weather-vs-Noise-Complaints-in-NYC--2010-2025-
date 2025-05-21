import requests
import csv
from datetime import datetime, timedelta

# Define time range
start_date = datetime(2010, 1, 1)
end_date = datetime(2025, 1, 1)
output_file = "hourly_weather.csv"

# Write header
with open(output_file, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["datetime", "temperature_2m", "precipitation", "wind_speed_10m"])

# Loop month by month
current = start_date
while current < end_date:
    chunk_start = current
    chunk_end = min(current + timedelta(days=30), end_date)

    params = {
        "latitude": 40.78,
        "longitude": -73.97,
        "start_date": chunk_start.strftime("%Y-%m-%d"),
        "end_date": chunk_end.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "America/New_York"
    }

    url = "https://archive-api.open-meteo.com/v1/archive"
    print(f"🔄 Fetching: {params['start_date']} to {params['end_date']}")

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        hours = data.get("hourly", {}).get("time", [])
        temps = data["hourly"].get("temperature_2m", [])
        precs = data["hourly"].get("precipitation", [])
        winds = data["hourly"].get("wind_speed_10m", [])

        if hours:
            with open(output_file, "a", newline="") as file:
                writer = csv.writer(file)
                for i in range(len(hours)):
                    writer.writerow([hours[i], temps[i], precs[i], winds[i]])
        else:
            print("⚠️ No hourly data returned — API call returned empty.")

    else:
        print("❌ API error:", response.status_code)

    current += timedelta(days=30)

print("✅ DONE: Data saved to", output_file)
