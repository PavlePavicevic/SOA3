import os
import time
import json
import pandas as pd
import requests

DEVICE_REST_URL = os.getenv("DEVICE_REST_URL", "http://localhost:59986")  
DEVICE_NAME = os.getenv("DEVICE_NAME", "WeatherDevice")

CSV_PATH = os.getenv("CSV_PATH", "./data/GlobalWeatherRepository.csv")
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "1.0"))

def pick_first(row, candidates, default=None):
    for c in candidates:
        if c in row and pd.notna(row[c]):
            return row[c]
    return default

def extract_temperature(row):
    return float(pick_first(row, ["temperature_celsius"]))

def extract_humidity(row):
    return float(pick_first(row, ["humidity"]))

def extract_pressure(row):
    return float(pick_first(row, ["pressure_mb"]))

def extract_wind_speed(row):
    return float(pick_first(row, ["wind_kph"]))

def send_readings(temp, hum, pres, wind, cooling_on=False):
    url = f"{DEVICE_REST_URL}/api/v3/reading"

    readings = [
        {"deviceName": DEVICE_NAME, "resourceName": "temperature", "valueType": "Float32", "value": str(temp)},
        {"deviceName": DEVICE_NAME, "resourceName": "humidity", "valueType": "Float32", "value": str(hum)},
        {"deviceName": DEVICE_NAME, "resourceName": "pressure", "valueType": "Float32", "value": str(pres)},
        {"deviceName": DEVICE_NAME, "resourceName": "wind_speed", "valueType": "Float32", "value": str(wind)},
        {"deviceName": DEVICE_NAME, "resourceName": "cooling_on", "valueType": "Bool", "value": "true" if cooling_on else "false"},
    ]

    payload = {"readings": readings}
    r = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(payload), timeout=10)
    r.raise_for_status()
    return r.status_code

def main():
    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows from {CSV_PATH}")

    idx = 0
    cooling_on = False

    while True:
        row = df.iloc[idx].to_dict()

        temp = extract_temperature(row)
        hum = extract_humidity(row)
        pres = extract_pressure(row)
        wind = extract_wind_speed(row)

        if temp > 30:
            cooling_on = True

        code = send_readings(temp, hum, pres, wind, cooling_on=cooling_on)
        print(f"[{idx}] sent readings (temp={temp}) status={code}")

        idx = (idx + 1) % len(df)
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
