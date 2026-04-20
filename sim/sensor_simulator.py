#!/usr/bin/env python3
"""
sensor_simulator.py

Simulates 20 independent sensors publishing at a combined 100 Hz via MQTT.
Each sensor publishes to topic:  sensors/<sensor_id>/data
Payload (JSON):  {"sensor_id": "...", "value": ..., "unit": "..."}

Anomaly injection: every ~30 seconds per sensor a spike is injected
(value = mean + 8 * stddev) to exercise the RT anomaly detector.

Usage:
    pip install paho-mqtt
    python3 sensor_simulator.py [--host localhost] [--port 1883] [--hz 100]
"""

import argparse
import json
import math
import random
import sys
import threading
import time

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Sensor definitions
# ---------------------------------------------------------------------------

SENSORS = [
    {"id": "temp_01",     "type": "temperature", "unit": "degC",  "mean": 22.0,  "stddev": 1.5},
    {"id": "temp_02",     "type": "temperature", "unit": "degC",  "mean": 35.0,  "stddev": 2.0},
    {"id": "temp_03",     "type": "temperature", "unit": "degC",  "mean": 18.0,  "stddev": 0.8},
    {"id": "temp_04",     "type": "temperature", "unit": "degC",  "mean": 55.0,  "stddev": 3.0},
    {"id": "pressure_01", "type": "pressure",    "unit": "kPa",   "mean": 101.3, "stddev": 0.5},
    {"id": "pressure_02", "type": "pressure",    "unit": "kPa",   "mean": 150.0, "stddev": 2.0},
    {"id": "humidity_01", "type": "humidity",    "unit": "%RH",   "mean": 60.0,  "stddev": 5.0},
    {"id": "humidity_02", "type": "humidity",    "unit": "%RH",   "mean": 45.0,  "stddev": 3.0},
    {"id": "vibration_01","type": "vibration",   "unit": "m/s2",  "mean": 0.5,   "stddev": 0.1},
    {"id": "vibration_02","type": "vibration",   "unit": "m/s2",  "mean": 1.2,   "stddev": 0.3},
    {"id": "current_01",  "type": "current",     "unit": "A",     "mean": 15.0,  "stddev": 0.8},
    {"id": "current_02",  "type": "current",     "unit": "A",     "mean": 8.5,   "stddev": 0.4},
    {"id": "voltage_01",  "type": "voltage",     "unit": "V",     "mean": 230.0, "stddev": 1.5},
    {"id": "voltage_02",  "type": "voltage",     "unit": "V",     "mean": 48.0,  "stddev": 0.5},
    {"id": "temp_05",     "type": "temperature", "unit": "degC",  "mean": 70.0,  "stddev": 4.0},
    {"id": "temp_06",     "type": "temperature", "unit": "degC",  "mean": 25.0,  "stddev": 1.0},
    {"id": "pressure_03", "type": "pressure",    "unit": "kPa",   "mean": 200.0, "stddev": 3.0},
    {"id": "humidity_03", "type": "humidity",    "unit": "%RH",   "mean": 75.0,  "stddev": 6.0},
    {"id": "vibration_03","type": "vibration",   "unit": "m/s2",  "mean": 2.0,   "stddev": 0.5},
    {"id": "current_03",  "type": "current",     "unit": "A",     "mean": 30.0,  "stddev": 1.5},
]

assert len(SENSORS) == 20, "Expected exactly 20 sensors"

# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class SensorSimulator:
    def __init__(self, host: str, port: int, total_hz: int):
        self.host      = host
        self.port      = port
        self.hz        = total_hz
        self.period_s  = 1.0 / total_hz    # inter-message interval (round-robin)
        self._stop     = threading.Event()
        self._counts   = {s["id"]: 0 for s in SENSORS}
        self._next_spike = {s["id"]: time.monotonic() + random.uniform(15, 45)
                            for s in SENSORS}

        self.client = mqtt.Client(client_id="sensor_simulator", clean_session=True)
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[sim] Connected to {self.host}:{self.port}")
        else:
            print(f"[sim] Connection failed: rc={rc}", file=sys.stderr)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print(f"[sim] Unexpected disconnect: rc={rc}", file=sys.stderr)

    def _sample(self, sensor: dict, now: float) -> float:
        """Generate a sample, injecting anomaly spikes periodically."""
        sid = sensor["id"]
        if now >= self._next_spike[sid]:
            # Spike: mean + 8 * stddev
            value = sensor["mean"] + 8.0 * sensor["stddev"]
            self._next_spike[sid] = now + random.uniform(25, 45)
            print(f"[sim] SPIKE injected: {sid} = {value:.3f} {sensor['unit']}")
        else:
            value = random.gauss(sensor["mean"], sensor["stddev"])
        return round(value, 4)

    def run(self):
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

        sensor_idx = 0
        next_publish = time.monotonic()

        print(f"[sim] Publishing {len(SENSORS)} sensors at {self.hz} msg/s total "
              f"(period={self.period_s*1000:.1f} ms)")

        try:
            while not self._stop.is_set():
                now = time.monotonic()

                # Busy-spin toward next publish time for sub-millisecond accuracy
                if now < next_publish:
                    slack = next_publish - now
                    if slack > 0.001:
                        time.sleep(slack * 0.9)
                    continue

                sensor = SENSORS[sensor_idx % len(SENSORS)]
                sensor_idx += 1
                next_publish += self.period_s

                value   = self._sample(sensor, now)
                payload = json.dumps({
                    "sensor_id": sensor["id"],
                    "value":     value,
                    "unit":      sensor["unit"],
                })
                topic = f"sensors/{sensor['id']}/data"
                self.client.publish(topic, payload, qos=0)
                self._counts[sensor["id"]] += 1

        except KeyboardInterrupt:
            pass
        finally:
            self.client.loop_stop()
            self.client.disconnect()

        total = sum(self._counts.values())
        print(f"[sim] Stopped — {total} messages published")
        for sid, cnt in self._counts.items():
            print(f"  {sid}: {cnt}")

    def stop(self):
        self._stop.set()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SensorExpress MQTT simulator")
    parser.add_argument("--host", default="localhost",
                        help="MQTT broker hostname (default: localhost)")
    parser.add_argument("--port", type=int, default=1883,
                        help="MQTT broker port (default: 1883)")
    parser.add_argument("--hz", type=int, default=100,
                        help="Total publish rate across all sensors (default: 100)")
    args = parser.parse_args()

    sim = SensorSimulator(args.host, args.port, args.hz)
    sim.run()


if __name__ == "__main__":
    main()
