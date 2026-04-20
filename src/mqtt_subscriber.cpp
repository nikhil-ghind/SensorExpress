/**
 * mqtt_subscriber.cpp
 *
 * Connects to an MQTT broker, subscribes to "sensors/+/data", parses
 * JSON payloads into SensorReading structs, and pushes them onto a
 * shared queue consumed by the RT processing thread.
 *
 * Payload format (JSON):
 *   { "sensor_id": "temp_01", "value": 23.4, "unit": "degC" }
 *
 * Dependencies: libmosquitto, nlohmann/json
 */

#include "sensor_express/pipeline.hpp"

#include <mosquitto.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

namespace sensor_express {

// ---------------------------------------------------------------------------
// Shared ingestion queue — written by MQTT callbacks, read by RT thread
// ---------------------------------------------------------------------------

static std::queue<SensorReading>  g_queue;
static std::mutex                 g_queue_mutex;
static std::condition_variable    g_queue_cv;
static std::atomic<bool>          g_running{false};

// ---------------------------------------------------------------------------
// Mosquitto callbacks
// ---------------------------------------------------------------------------

static void on_connect(struct mosquitto* /*mosq*/, void* /*obj*/, int rc) {
    if (rc == 0) {
        spdlog::info("[mqtt] Connected to broker (rc=0)");
    } else {
        spdlog::error("[mqtt] Connection refused: rc={}", rc);
    }
}

static void on_disconnect(struct mosquitto* /*mosq*/, void* /*obj*/, int rc) {
    if (rc != 0) {
        spdlog::warn("[mqtt] Unexpected disconnect (rc={}), will reconnect", rc);
    }
}

static void on_message(struct mosquitto* /*mosq*/, void* /*obj*/,
                       const struct mosquitto_message* msg) {
    if (!msg || !msg->payload) return;

    try {
        auto j = nlohmann::json::parse(
            static_cast<const char*>(msg->payload),
            static_cast<const char*>(msg->payload) + msg->payloadlen);

        SensorReading reading;
        reading.sensor_id    = j.at("sensor_id").get<std::string>();
        reading.value        = j.at("value").get<double>();
        reading.unit         = j.value("unit", "");

        // Capture ingestion timestamp (CLOCK_MONOTONIC)
        struct timespec ts{};
        clock_gettime(CLOCK_MONOTONIC, &ts);
        reading.timestamp_ns =
            static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;

        // Infer sensor type from topic or unit
        std::string topic(msg->topic);
        if (topic.find("temp") != std::string::npos ||
            reading.unit == "degC" || reading.unit == "degF") {
            reading.type = SensorType::TEMPERATURE;
        } else if (topic.find("pressure") != std::string::npos) {
            reading.type = SensorType::PRESSURE;
        } else if (topic.find("humidity") != std::string::npos) {
            reading.type = SensorType::HUMIDITY;
        } else if (topic.find("vibration") != std::string::npos) {
            reading.type = SensorType::VIBRATION;
        }

        {
            std::lock_guard<std::mutex> lk(g_queue_mutex);
            g_queue.push(std::move(reading));
        }
        g_queue_cv.notify_one();

    } catch (const std::exception& ex) {
        spdlog::warn("[mqtt] Failed to parse message on {}: {}", msg->topic, ex.what());
    }
}

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

/**
 * Block until at least one reading is available, then move all queued
 * readings into `out`.  Returns the number of readings transferred.
 */
size_t drain_queue(std::vector<SensorReading>& out, int timeout_ms = 100) {
    std::unique_lock<std::mutex> lk(g_queue_mutex);
    g_queue_cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                        [] { return !g_queue.empty() || !g_running.load(); });
    size_t n = 0;
    while (!g_queue.empty()) {
        out.push_back(std::move(g_queue.front()));
        g_queue.pop();
        ++n;
    }
    return n;
}

/**
 * Run the MQTT subscriber loop in the calling thread.
 * Reconnects with exponential backoff on failure.
 * Stops when g_running is set to false.
 */
void run_mqtt_subscriber(const PipelineConfig& cfg) {
    mosquitto_lib_init();
    g_running = true;

    struct mosquitto* mosq =
        mosquitto_new(cfg.client_id.c_str(), /*clean_session=*/true, nullptr);
    if (!mosq) {
        spdlog::critical("[mqtt] mosquitto_new() failed — out of memory");
        mosquitto_lib_cleanup();
        return;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_message_callback_set(mosq, on_message);

    int backoff_s = 1;
    constexpr int kMaxBackoff = 30;

    while (g_running.load()) {
        int rc = mosquitto_connect(mosq, cfg.mqtt_host.c_str(),
                                   cfg.mqtt_port, /*keepalive=*/60);
        if (rc != MOSQ_ERR_SUCCESS) {
            spdlog::error("[mqtt] Connect to {}:{} failed: {} — retry in {}s",
                          cfg.mqtt_host, cfg.mqtt_port,
                          mosquitto_strerror(rc), backoff_s);
            std::this_thread::sleep_for(std::chrono::seconds(backoff_s));
            backoff_s = std::min(backoff_s * 2, kMaxBackoff);
            continue;
        }
        backoff_s = 1;  // reset on success

        // Subscribe after connect
        rc = mosquitto_subscribe(mosq, nullptr,
                                 cfg.mqtt_topic_pattern.c_str(), /*qos=*/0);
        if (rc != MOSQ_ERR_SUCCESS) {
            spdlog::error("[mqtt] Subscribe failed: {}", mosquitto_strerror(rc));
        } else {
            spdlog::info("[mqtt] Subscribed to '{}'", cfg.mqtt_topic_pattern);
        }

        // Loop until disconnect or stop
        rc = mosquitto_loop_forever(mosq, /*timeout=*/100, /*max_packets=*/1);
        if (g_running.load() && rc != MOSQ_ERR_SUCCESS) {
            spdlog::warn("[mqtt] Loop exited: {} — reconnecting in {}s",
                         mosquitto_strerror(rc), backoff_s);
            std::this_thread::sleep_for(std::chrono::seconds(backoff_s));
            backoff_s = std::min(backoff_s * 2, kMaxBackoff);
        }
    }

    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    spdlog::info("[mqtt] Subscriber stopped");
}

/** Signal the subscriber thread to stop. */
void stop_mqtt_subscriber() {
    g_running = false;
    g_queue_cv.notify_all();
}

}  // namespace sensor_express
