# SensorExpress

A C++17 real-time sensor data pipeline with 5 ms hard deadline guarantees, SCHED_FIFO scheduling, and online anomaly detection via Welford z-score statistics.

---

## Architecture

```mermaid
flowchart TB
    broker["MQTT broker<br/>topic sensors/+/data"]

    subgraph nonrt["Non-RT threads"]
        sub["mqtt_subscriber<br/>libmosquitto callback<br/>JSON to SensorReading"]
        q["Shared std::queue<br/>mutex + condition_variable"]
    end

    subgraph rt["RT thread — SCHED_FIFO prio 80, pinned to rt_cpu"]
        timer["DeadlineTimer<br/>clock_nanosleep TIMER_ABSTIME<br/>1 ms period"]
        drain["drain_queue<br/>non-blocking, max 50 readings/tick"]
        stats["RollingStats per sensor_id<br/>Welford mean/variance, O(1)"]
        judge{"abs z >= anomaly_threshold?"}
    end

    subgraph out["Output threads"]
        alert["alert_dispatcher<br/>libcurl HTTP POST webhook<br/>high prio immediate, low prio batched"]
        metrics["metrics_server<br/>cpp-httplib on metrics_port<br/>GET /metrics"]
    end

    main["main.cpp<br/>mlockall, YAML config,<br/>SIGINT/SIGTERM shutdown"]

    broker --> sub --> q --> drain
    timer --> drain --> stats --> judge
    judge -->|"yes"| cb["result callback in main"]
    judge -->|"no"| cb
    cb --> alert
    cb --> metrics
    drain -. "cycle > deadline_ms" .-> miss["DeadlineMissed callback"]
    miss --> metrics
    main -.->|"starts + configures"| nonrt
    main -.-> rt
    main -.-> out
```

A tick of the RT loop looks like this:

```mermaid
sequenceDiagram
    participant T as DeadlineTimer
    participant P as rt_processor
    participant Q as reading queue
    participant S as RollingStats
    participant M as main callbacks
    participant A as alert_dispatcher

    T->>P: wake at absolute deadline (1 ms period)
    P->>Q: drain_queue(timeout 0)
    Q-->>P: up to 50 SensorReading
    loop per reading
        P->>S: update(value)
        S-->>P: mean, stddev, z-score
        P->>M: ProcessingResult (latency_ns, anomaly)
        alt abs(z) >= alert_threshold
            M->>A: enqueue_alert(high priority)
            A->>A: POST webhook immediately
        else abs(z) >= anomaly_threshold
            M->>A: enqueue_alert(low priority)
            A->>A: hold for batch flush
        end
    end
    P->>M: DeadlineMissed if cycle exceeded deadline_ms
```

---

## Real-Time Design

### SCHED_FIFO Scheduling

The processing thread runs under Linux's `SCHED_FIFO` policy at priority 80:

```cpp
struct sched_param param{ .sched_priority = 80 };
sched_setscheduler(0, SCHED_FIFO, &param);
```

`SCHED_FIFO` threads are never preempted by lower-priority tasks and don't get time-sliced. This guarantees the processor gets CPU time within microseconds of its wakeup.

### CPU Affinity

The RT thread is pinned to CPU 1 via `pthread_setaffinity_np`, isolating it from OS housekeeping tasks (typically on CPU 0) and avoiding cache-line contention.

### Memory Locking

`mlockall(MCL_CURRENT | MCL_FUTURE)` is called at startup before any RT threads are created. This pre-faults all pages into RAM, preventing page-fault latency spikes inside the 5 ms deadline window.

### 5 ms Deadline Guarantee

The `DeadlineTimer` uses `clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, ...)` with absolute wakeup times. Unlike relative sleeps, absolute deadlines don't accumulate drift across cycles. If a cycle overruns, the timer skips forward rather than cascading latency.

---

## Anomaly Detection

Online Welford statistics maintain per-sensor rolling mean and variance in O(1) time and O(1) space — no buffer required.

```
z = (value - mean) / stddev
```

| Threshold | Action |
|-----------|--------|
| `\|z\| ≥ 3.0` | Flag `ProcessingResult.anomaly = true` |
| `\|z\| ≥ 4.0` | Immediate HTTP POST to webhook |
| `\|z\| ≥ 3.0` (low) | Batched every 10 s |

A 30-sample warm-up prevents spurious anomalies while the estimator converges.

---

## Prometheus Metrics

Scraped at `http://localhost:9090/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `sensor_express_readings_processed_total` | Counter | Total readings through RT pipeline |
| `sensor_express_deadline_misses_total` | Counter | Cycles exceeding 5 ms deadline |
| `sensor_express_anomalies_detected_total` | Counter | Flagged anomalous readings |
| `sensor_express_processing_latency_ns` | Histogram | Per-reading processing time (ns) |

---

## Build

### Prerequisites

- Linux kernel ≥ 4.14 (SCHED_FIFO + `clock_nanosleep`)
- CMake ≥ 3.16, GCC/Clang with C++17
- `libmosquitto-dev`, `libyaml-cpp-dev`, `libspdlog-dev`, `libcurl4-openssl-dev`
- `nlohmann-json3-dev`, `libgtest-dev`
- `httplib.h` at `/usr/local/include/httplib.h`

```bash
sudo apt install libmosquitto-dev libyaml-cpp-dev libspdlog-dev \
                 libcurl4-openssl-dev libnlohmann-json3-dev libgtest-dev

# httplib single-header
sudo curl -o /usr/local/include/httplib.h \
    https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h

cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Run tests

```bash
cd build && ctest --output-on-failure
```

---

## Run with Docker Compose

```bash
cd docker
docker compose up --build
```

Services:

| Service | Port | Description |
|---------|------|-------------|
| `mosquitto` | 1883 | MQTT broker |
| `sensor_express` | 9090 | RT pipeline + metrics |
| `simulator` | — | 20 sensors at 100 Hz |
| `prometheus` | 9091 | Metrics scraper |
| `grafana` | 3000 | Dashboard (admin/admin) |

> **Note:** The `sensor_express` service uses `network_mode: host` and `cap_add: [SYS_NICE, IPC_LOCK]` for RT scheduling. Docker Desktop on macOS/Windows does not support `SCHED_FIFO` — use a Linux host or VM.

---

## Run manually

```bash
# Start Mosquitto
mosquitto -c /etc/mosquitto/mosquitto.conf &

# Start pipeline (needs CAP_SYS_NICE for SCHED_FIFO)
sudo ./build/sensor_express config/pipeline.yaml

# Start simulator
python3 sim/sensor_simulator.py --host localhost --hz 100

# Check metrics
curl http://localhost:9090/metrics
```

---

## Configuration

See `config/pipeline.yaml` for all tuneable parameters including MQTT broker address, RT priority/CPU, deadline threshold, anomaly z-score thresholds, webhook URL, and log rotation settings.
