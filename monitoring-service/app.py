import os
import json
import time
import threading
import requests
import paho.mqtt.client as mqtt
from flask import Flask, request, jsonify

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "edgex-events")

COMMAND_SERVICE_URL = os.getenv("COMMAND_SERVICE_URL", "http://command-service:8090")
DEFAULT_TEMP_THRESHOLD = float(os.getenv("DEFAULT_TEMP_THRESHOLD", "30"))

state = {
    "temp_threshold": DEFAULT_TEMP_THRESHOLD,
    "last_temperature": None,
    "cooling_on": False
}

app = Flask(__name__)

def parse_temperature(payload: dict):
    readings = payload.get("readings", [])
    for r in readings:
        if r.get("resourceName") == "temperature":
            try:
                return float(r.get("value"))
            except Exception:
                return None
    return None

def set_cooling(on: bool):
    url = f"{COMMAND_SERVICE_URL}/cooling"
    r = requests.post(url, json={"on": on}, timeout=5)
    r.raise_for_status()
    state["cooling_on"] = on
    return r.json()

def on_message(_client, _userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    temp = parse_temperature(payload)
    if temp is None:
        return

    state["last_temperature"] = temp

    if temp > state["temp_threshold"] and not state["cooling_on"]:
        print(f"[monitoring] temp {temp} > {state['temp_threshold']} => cooling ON")
        try:
            set_cooling(True)
        except Exception as e:
            print("[monitoring] failed to set cooling ON:", e)

    if temp <= state["temp_threshold"] and state["cooling_on"]:
        print(f"[monitoring] temp {temp} <= {state['temp_threshold']} => cooling OFF")
        try:
            set_cooling(False)
        except Exception as e:
            print("[monitoring] failed to set cooling OFF:", e)

@app.put("/rules/temp-threshold")
def update_threshold():
    data = request.get_json(force=True) or {}
    thr = float(data["threshold"])
    state["temp_threshold"] = thr
    return jsonify({"ok": True, "temp_threshold": state["temp_threshold"]})

@app.get("/status")
def status():
    return jsonify(state)

@app.get("/health")
def health():
    return jsonify({"ok": True})

def mqtt_thread():
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
    print(f"[monitoring] subscribed to {MQTT_TOPIC}")
    m.loop_forever()

def main():
    t = threading.Thread(target=mqtt_thread, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8091)

if __name__ == "__main__":
    main()
