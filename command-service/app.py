import os
import requests
from flask import Flask, request, jsonify

EDGEX_CORE_COMMAND_URL = os.getenv("EDGEX_CORE_COMMAND_URL", "http://edgex-core-command:59882")
DEVICE_NAME = os.getenv("DEVICE_NAME", "WeatherDevice")

app = Flask(__name__)

@app.post("/cooling")
def cooling():
    data = request.get_json(force=True)
    on = bool(data.get("on", False))

    url = f"{EDGEX_CORE_COMMAND_URL}/api/v3/device/name/{DEVICE_NAME}/SetCooling"
    body = {"cooling_on": "true" if on else "false"}

    try:
        r = requests.put(url, json=body, timeout=10)
        r.raise_for_status()
        return jsonify({"ok": True, "sent_to_edgex": True, "on": on})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "on": on}), 500

@app.get("/health")
def health():
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8090)
