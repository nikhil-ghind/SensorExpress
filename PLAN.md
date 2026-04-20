# Sensor Express

## Project Overview

A multi-threaded C++ pipeline running on Linux/RTOS that ingests sensor data over MQTT, processes it through a staged pipeline, and enforces deterministic deadline guarantees under 5ms end-to-end. Uses POSIX threads with thread affinity tuning, priority scheduling, and multi-core synchronization primitives to achieve hard real-time performance.

**Key Goals:**
- Sub-5ms deterministic end-to-end latency from MQTT message receipt to processed output
- Multi-threaded pipeline with configurable stages (ingest, filter, transform, output)
- Thread affinity pinning and SCHED_FIFO priority scheduling for deterministic behavior
- Lock-free or minimal-lock inter-stage communication via ring buffers
- MQTT-based sensor data ingestion with QoS guarantees
- NTP-synchronized timestamps for cross-node correlation

## Tech Stack

- C++17 (GCC 12+ or Clang 15+)
- POSIX Threads (pthread), SCHED_FIFO/SCHED_RR
- Eclipse Paho MQTT C++ Client 1.3+
- Linux (kernel 5.15+ with PREEMPT_RT patch for RTOS mode)
- CMake 3.22+
- Google Test (gtest) 1.14 for unit testing
- NTP / chrony for time synchronization
- Optional: perf, ftrace for latency profiling

## Architecture Overview

```
[Sensors] --MQTT--> [MQTT Subscriber Thread]
                           |
                     [Ring Buffer 1]
                           |
                    [Filter Thread]
                           |
                     [Ring Buffer 2]
                           |
                   [Transform Thread]
                           |
                     [Ring Buffer 3]
                           |
                    [Output Thread] --> [downstream consumer / file / network]
                           
[Watchdog Thread] monitors all stages for deadline violations
[Stats Thread] collects latency histograms
```

**Components:**
1. **MQTT Subscriber** - Receives sensor data, stamps arrival time, enqueues to ring buffer.
2. **Filter Stage** - Validates data, applies range checks, drops invalid readings.
3. **Transform Stage** - Applies calibration, unit conversion, moving average smoothing.
4. **Output Stage** - Publishes processed data to downstream (MQTT publish, shared memory, or file).
5. **Ring Buffers** - Lock-free SPSC (single-producer single-consumer) queues between stages.
6. **Watchdog Thread** - Monitors per-message deadlines, logs violations, triggers alerts.
7. **Stats Collector** - Tracks p50/p95/p99/max latencies per stage.

---

## Phase 1: Project Setup and Lock-Free Ring Buffer

**Goal:** Set up the CMake build system and implement the foundational lock-free SPSC ring buffer.

### Tasks

1. Create `CMakeLists.txt` (root):
   - Set cmake_minimum_required(VERSION 3.22), project name `sensor_pipeline`.
   - Set C++17 standard, enable warnings (-Wall -Wextra -Wpedantic).
   - Add subdirectories: src, tests.
   - Find packages: Threads (required), PahoMqttCpp.

2. Create `src/CMakeLists.txt`:
   - Add library `pipeline_lib` from source files.
   - Link Threads::Threads, PahoMqttCpp::paho-mqttcpp3.

3. Create `tests/CMakeLists.txt`:
   - Fetch GoogleTest via FetchContent.
   - Add test executables linking pipeline_lib and GTest::gtest_main.

4. Create `include/pipeline/ring_buffer.hpp`:
   - Template class `RingBuffer<T, Size>`.
   - Lock-free SPSC queue using std::atomic for head/tail indices.
   - Methods: `bool try_push(const T& item)`, `bool try_pop(T& item)`, `size_t size() const`, `bool empty() const`.
   - Use `std::atomic_thread_fence(std::memory_order_acquire/release)` for correct ordering.
   - Cache-line padding (alignas(64)) on head and tail to prevent false sharing.

5. Create `include/pipeline/sensor_data.hpp`:
   - Struct `SensorReading`: fields `uint32_t sensor_id`, `double value`, `uint64_t timestamp_ns` (NTP-synced nanoseconds), `uint64_t arrival_ns` (local monotonic clock), `uint8_t quality_flag`.
   - Struct `PipelineMessage`: wraps `SensorReading` plus `uint64_t deadline_ns` (arrival_ns + 5000000).

6. Create `tests/test_ring_buffer.cpp`:
   - Test single-thread push/pop.
   - Test buffer full returns false on try_push.
   - Test buffer empty returns false on try_pop.
   - Test concurrent SPSC correctness: one producer thread, one consumer thread, verify all items received in order.

7. Build and run: `mkdir build && cd build && cmake .. && make -j$(nproc) && ctest --output-on-failure`.

---

## Phase 2: MQTT Subscriber and Ingest Thread

**Goal:** Connect to an MQTT broker, subscribe to sensor topics, and enqueue readings into the pipeline.

### Tasks

1. Create `include/pipeline/mqtt_subscriber.hpp` and `src/mqtt_subscriber.cpp`:
   - Class `MqttSubscriber` wrapping `mqtt::async_client`.
   - Constructor: takes broker URI, client ID, topic filter, QoS level.
   - Method: `void start(RingBuffer<PipelineMessage, N>& output_buffer)`.
   - Implements `mqtt::callback` interface: `message_arrived()` parses JSON payload into SensorReading, stamps `arrival_ns` using `clock_gettime(CLOCK_MONOTONIC)`, computes `deadline_ns = arrival_ns + 5000000`, pushes PipelineMessage to ring buffer.
   - Method: `void stop()` - disconnects cleanly.

2. Create `include/pipeline/json_parser.hpp` and `src/json_parser.cpp`:
   - Lightweight JSON parser for sensor data (use nlohmann/json or manual parsing for minimal overhead).
   - Function: `SensorReading parse_sensor_json(const std::string& payload)`.
   - Expected format: `{"sensor_id": 42, "value": 23.5, "timestamp": 1700000000000000000, "quality": 1}`.

3. Create `include/pipeline/thread_utils.hpp` and `src/thread_utils.cpp`:
   - Function: `void set_thread_affinity(pthread_t thread, int core_id)` - uses `pthread_setaffinity_np`.
   - Function: `void set_thread_priority(pthread_t thread, int priority, int policy = SCHED_FIFO)` - uses `pthread_setschedparam`.
   - Function: `uint64_t now_monotonic_ns()` - wraps `clock_gettime(CLOCK_MONOTONIC)`.
   - Function: `void set_thread_name(const std::string& name)` - uses `pthread_setname_np`.

4. Create `config/pipeline_config.yaml`:
   - Configuration: mqtt.broker_uri, mqtt.topic, mqtt.qos, pipeline.deadline_us: 5000, pipeline.buffer_size: 4096, threads.ingest_core: 0, threads.filter_core: 1, threads.transform_core: 2, threads.output_core: 3, threads.priority: 80.

5. Create `include/pipeline/config.hpp` and `src/config.cpp`:
   - Class `PipelineConfig` that loads YAML config (use yaml-cpp or simple custom parser).
   - Getters for all config fields.

6. Create `tests/test_mqtt_subscriber.cpp`:
   - Mock MQTT client (or use test broker).
   - Verify SensorReading correctly parsed and enqueued.
   - Verify arrival_ns is stamped.

---

## Phase 3: Pipeline Stages (Filter, Transform, Output)

**Goal:** Implement the processing stages that run as dedicated threads with affinity pinning.

### Tasks

1. Create `include/pipeline/stage.hpp`:
   - Abstract base class `PipelineStage`:
     - Virtual method: `bool process(PipelineMessage& msg)` - returns false to drop message.
     - Method: `void run(RingBuffer<PipelineMessage, N>& input, RingBuffer<PipelineMessage, N>& output)` - loop: try_pop from input, call process(), if not dropped try_push to output. Uses busy-wait with `_mm_pause()` or `sched_yield()` on empty buffer.

2. Create `include/pipeline/filter_stage.hpp` and `src/filter_stage.cpp`:
   - Class `FilterStage : public PipelineStage`.
   - `bool process(PipelineMessage& msg)`:
     - Check `msg.reading.quality_flag != 0` (drop invalid).
     - Check value within configured min/max range (drop out-of-range).
     - Check deadline not already exceeded (drop if `now_monotonic_ns() > msg.deadline_ns`).
     - Return true to keep, false to drop.

3. Create `include/pipeline/transform_stage.hpp` and `src/transform_stage.cpp`:
   - Class `TransformStage : public PipelineStage`.
   - `bool process(PipelineMessage& msg)`:
     - Apply calibration offset and scale factor from config: `value = value * scale + offset`.
     - Apply exponential moving average (EMA) with configurable alpha per sensor_id (use unordered_map<uint32_t, double> for state).
     - Return true always.

4. Create `include/pipeline/output_stage.hpp` and `src/output_stage.cpp`:
   - Class `OutputStage`:
     - Method: `void run(RingBuffer<PipelineMessage, N>& input)` - consumes from input, writes processed reading to output sink.
     - Output sinks (configurable): MQTT publish, write to binary file, write to shared memory segment.
     - Logs deadline violations if `now_monotonic_ns() > msg.deadline_ns`.

5. Create `tests/test_filter_stage.cpp`:
   - Test valid reading passes through.
   - Test quality_flag=0 is dropped.
   - Test out-of-range value is dropped.

6. Create `tests/test_transform_stage.cpp`:
   - Test calibration math.
   - Test EMA convergence over multiple readings.

---

## Phase 4: Pipeline Orchestrator and Thread Management

**Goal:** Wire all stages together, spawn threads with affinity and priority, manage lifecycle.

### Tasks

1. Create `include/pipeline/pipeline.hpp` and `src/pipeline.cpp`:
   - Class `Pipeline`:
     - Owns ring buffers between stages: `RingBuffer<PipelineMessage, 4096> buf_ingest_filter_`, `buf_filter_transform_`, `buf_transform_output_`.
     - Owns stage instances: `MqttSubscriber`, `FilterStage`, `TransformStage`, `OutputStage`.
     - Method: `void start(const PipelineConfig& config)`:
       - Spawn pthread for each stage.
       - Pin ingest thread to core 0, filter to core 1, transform to core 2, output to core 3 (configurable).
       - Set all threads to SCHED_FIFO priority 80 (configurable).
       - Set thread names for debugging.
     - Method: `void stop()` - set atomic `running_` flag to false, join all threads.
     - Method: `bool is_running() const`.

2. Create `src/main.cpp`:
   - Parse command-line args for config file path.
   - Load PipelineConfig.
   - Instantiate Pipeline, call start().
   - Install signal handler for SIGINT/SIGTERM to call stop().
   - Main thread blocks on `sigwait()` or condition variable until shutdown.

3. Create `include/pipeline/watchdog.hpp` and `src/watchdog.cpp`:
   - Class `Watchdog`:
     - Runs as a separate thread at lower priority.
     - Each stage reports heartbeat (atomic timestamp) on every N-th message processed.
     - Watchdog checks heartbeats every 100ms; if any stage has not reported for > 500ms, logs a stall warning.
     - Tracks deadline violation counter (atomic) incremented by output stage.

4. Add to CMakeLists: link all sources, create `sensor_pipeline` executable from main.cpp.

5. Create `tests/test_pipeline_integration.cpp`:
   - Spin up pipeline with mock MQTT input (direct ring buffer injection).
   - Push 1000 sensor readings, verify all arrive at output in order with correct transformations.
   - Verify latency (end-to-end) is under 5ms for all readings when running with sufficient priority.

---

## Phase 5: Latency Measurement and Stats Collection

**Goal:** Add instrumentation to measure and report per-stage and end-to-end latencies.

### Tasks

1. Create `include/pipeline/stats_collector.hpp` and `src/stats_collector.cpp`:
   - Class `StatsCollector`:
     - Method: `void record(const std::string& stage_name, uint64_t latency_ns)`.
     - Internally maintains per-stage histogram using fixed buckets (0-100us, 100-500us, 500us-1ms, 1-2ms, 2-5ms, >5ms).
     - Computes p50, p95, p99, max from ring buffer of last 10000 samples.
     - Method: `void print_report()` - prints formatted latency report to stdout or log file.
     - Thread-safe: uses per-stage atomic counters or a dedicated stats thread that consumes from a stats ring buffer.

2. Instrument each pipeline stage:
   - In `PipelineStage::run()`, record `uint64_t stage_start = now_monotonic_ns()` before `process()` and `stage_end` after, call `stats_collector.record(stage_name, stage_end - stage_start)`.
   - In output stage, record end-to-end latency: `now_monotonic_ns() - msg.reading.arrival_ns`.

3. Create `include/pipeline/latency_logger.hpp` and `src/latency_logger.cpp`:
   - Writes per-message latency to a binary log file for offline analysis.
   - Format: struct { uint64_t arrival_ns, uint64_t completion_ns, uint32_t sensor_id } packed.
   - Buffered writes (flush every 1000 entries) to avoid I/O stalls.

4. Create `scripts/analyze_latency.py`:
   - Python script that reads the binary latency log.
   - Computes and prints p50, p95, p99, max latencies.
   - Plots latency distribution histogram using matplotlib.
   - Usage: `python3 scripts/analyze_latency.py latency.bin`.

5. Create `tests/test_stats_collector.cpp`:
   - Test histogram bucket assignment.
   - Test percentile computation with known data.

---

## Phase 6: NTP Synchronization and Hardening

**Goal:** Ensure timestamps are NTP-synchronized across nodes, harden for production use.

### Tasks

1. Create `include/pipeline/time_sync.hpp` and `src/time_sync.cpp`:
   - Class `TimeSync`:
     - Method: `uint64_t get_wall_clock_ns()` - uses `clock_gettime(CLOCK_REALTIME)` for NTP-disciplined wall clock.
     - Method: `uint64_t get_monotonic_ns()` - uses `clock_gettime(CLOCK_MONOTONIC)` for latency measurement.
     - Method: `int64_t get_ntp_offset_us()` - reads NTP offset from `chronyc tracking` or `/var/run/chrony/chronyd.sock` for monitoring sync quality.
     - Method: `bool is_synced(int64_t max_offset_us = 1000)` - returns true if NTP offset is within threshold.

2. Create `config/chrony.conf.example`:
   - Example chrony configuration for sub-millisecond NTP sync.
   - Configure: `server pool.ntp.org iburst`, `makestep 0.1 3`, `rtcsync`.

3. Add startup check in `main.cpp`:
   - Verify NTP sync status via `TimeSync::is_synced()`.
   - Log warning if offset > 1ms, abort if offset > 10ms (configurable).

4. Add memory locking:
   - In `main.cpp`, call `mlockall(MCL_CURRENT | MCL_FUTURE)` to prevent page faults.
   - Pre-fault stack for each thread by touching stack pages in thread entry.

5. Add resource limit setup:
   - Create `config/limits.conf.example` - set `rtprio 99`, `memlock unlimited` for the pipeline user.

6. Create `scripts/setup_rt.sh`:
   - Script to configure system for real-time: set CPU governor to performance, disable CPU frequency scaling, isolate CPUs for pipeline threads (isolcpus boot param guidance), verify PREEMPT_RT kernel.

7. Create `tests/test_time_sync.cpp`:
   - Test monotonic clock is monotonically increasing across calls.
   - Test wall clock returns reasonable values.
