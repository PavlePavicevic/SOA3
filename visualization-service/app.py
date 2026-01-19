import os
import json
import time

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "edgex-events")

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "pro-token-123")
INFLUX_ORG = os.getenv("INFLUX_ORG", "pro")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "weather")
MEASUREMENT = os.getenv("INFLUX_MEASUREMENT", "weather_reading")

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()

def parse_edgex_event(payload: dict):
    
    event = payload.get("event", payload)
    readings = event.get("readings", [])
    device = event.get("deviceName", "unknown")
    origin = event.get("origin", None) 
    return device, origin, readings

def on_message(_client, _userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        print("JSON parse error:", e)
        return

    try:
        device, origin, readings = parse_edgex_event(payload)
        fields = {}
        for r in readings:
            rn = r.get("resourceName")
            val = r.get("value")
            if rn is None or val is None:
                continue
            if val.lower() in ["true", "false"]:
                fields[rn] = (val.lower() == "true")
            else:
                try:
                    fields[rn] = float(val)
                except:
                    fields[rn] = val

        if not fields:
            return

        p = Point(MEASUREMENT).tag("device", device)
        for k, v in fields.items():
            p = p.field(k, v)

        if isinstance(origin, int) and origin > 10_000_000_000:
            p = p.time(origin, WritePrecision.NS)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
        print("Wrote to influx:", fields)

    except Exception as e:
        print("Processing error:", e)

def main():
    m = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    m.on_message = on_message

    while True:
        try:
            m.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            break
        except Exception as e:
            print("MQTT connect failed, retrying:", e)
            time.sleep(2)

    m.subscribe(MQTT_TOPIC)
    print(f"Subscribed to {MQTT_TOPIC} on {MQTT_BROKER}:{MQTT_PORT}")
    m.loop_forever()

if __name__ == "__main__":
    main()
