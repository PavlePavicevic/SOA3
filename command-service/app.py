import os
import requests
from flask import Flask, request, jsonify

EDGEX_CORE_COMMAND_URL = os.getenv("EDGEX_CORE_COMMAND_URL", "http://edgex-core-command:59882")
DEVICE_NAME = os.getenv("DEVICE_NAME", "WeatherDevice")

app = Flask(__name__)

@app.post("/cooling")
def cooling():
    data = request.get_json(force=True) or {}
    if "on" not in data:
        return jsonify({"ok": False, "error": "Missing field: on"}), 400
    on = bool(data.get("on", False))

    url = f"{EDGEX_CORE_COMMAND_URL}/api/v3/device/name/{DEVICE_NAME}/SetCooling"
    body = {"cooling_on": True if on else False}

    print(f"[command-service] received cooling={on}. Would call: {url} body= {body}")


    sent_to_edgex = False
    edgex_error = None
    try:
        r = requests.put(url, json=body, timeout=10)
        if r.ok:
            sent_to_edgex = True
        else:
            edgex_error = f"{r.status_code} {r.text}"
        
    except Exception as e:
        edgex_error = str(e)
    
    return jsonify({"ok": True, "cooling_on":on, "sent_to_edgex": sent_to_edgex, "edgex_error":edgex_error})

cooling_state = {"cooling_on": False}

@app.put("/cooling_on")
def set_cooling_on():
    data = request.get_json(silent=True) or {}
    v = data.get("cooling_on", data.get("value", data.get("on", False)))

    if isinstance(v, str):
        v = v.lower() in ("true", "1", "yes", "on")

    cooling_state["cooling_on"] = bool(v)
    return jsonify({"ok": True, "cooling_on": cooling_state["cooling_on"]})

@app.get("/cooling_on")
def get_cooling_on():
    return jsonify(cooling_state)

@app.get("/health")
def health():
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8090)
