#!/usr/bin/env bash
set -e

echo "Seeding device profile..."
curl -sS -X POST \
  http://localhost:59881/api/v3/deviceprofile \
  -H "Content-Type: application/yaml" \
  --data-binary @./profiles/weather-device-profile.yaml

echo
echo "Seeding device..."
curl -sS -X POST \
  http://localhost:59881/api/v3/device \
  -H "Content-Type: application/json" \
  --data-binary @./devices/weather-device.json

echo
echo "Done."
