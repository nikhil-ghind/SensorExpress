/**
 * main.cpp — SensorExpress entry point
 *
 * Boot sequence:
 *   1. Load config from pipeline.yaml
 *   2. Lock process memory (mlockall) to prevent RT page faults
 *   3. Install SIGTERM / SIGINT handler
 *   4. Start threads: mqtt_subscriber, rt_processor, alert_dispatcher, metrics_server
 *   5. Wait for shutdown signal, then cleanly stop all threads
 */

#include "sensor_express/pipeline.hpp"
#include "sensor_express/realtime.hpp"

#include <yaml-cpp/yaml.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <atomic>
#include <csignal>
#include <stdexcept>
#include <string>
#include <thread>

// Forward declarations for thread entry points (defined in other TUs)
namespace sensor_express {
    void run_mqtt_subscriber(const PipelineConfig&);
    void stop_mqtt_subscriber();

    void run_rt_processor(const PipelineConfig&);
    void stop_rt_processor();
    void set_result_callback(std::function<void(const ProcessingResult&)>);
    void set_deadline_missed_callback(std::function<void(const DeadlineMissed&)>);

    void run_alert_dispatcher(const PipelineConfig&);
    void stop_alert_dispatcher();
    void enqueue_alert(const ProcessingResult&, bool high_priority);

    void run_metrics_server(const PipelineConfig&);
    void stop_metrics_server();
    void record_reading_processed(int64_t latency_ns);
    void record_deadline_miss();
    void record_anomaly();
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

static std::atomic<bool> g_shutdown{false};

static void signal_handler(int /*sig*/) {
    g_shutdown = true;
}

// ---------------------------------------------------------------------------
// Config loader
// ---------------------------------------------------------------------------

static sensor_express::PipelineConfig load_config(const std::string& path) {
    sensor_express::PipelineConfig cfg;
    try {
        YAML::Node y = YAML::LoadFile(path);

        auto get = [&](auto& field, const std::string& key) {
            if (y[key]) field = y[key].template as<std::decay_t<decltype(field)>>();
        };

        get(cfg.mqtt_host,               "mqtt_host");
        get(cfg.mqtt_port,               "mqtt_port");
        get(cfg.mqtt_topic_pattern,      "mqtt_topic_pattern");
        get(cfg.client_id,               "client_id");
        get(cfg.rt_priority,             "rt_priority");
        get(cfg.rt_cpu,                  "rt_cpu");
        get(cfg.deadline_ms,             "deadline_ms");
        get(cfg.anomaly_threshold,       "anomaly_threshold");
        get(cfg.alert_threshold,         "alert_threshold");
        get(cfg.webhook_url,             "webhook_url");
        get(cfg.alert_batch_interval_s,  "alert_batch_interval_s");
        get(cfg.metrics_port,            "metrics_port");
        get(cfg.log_path,                "log_path");
        get(cfg.log_max_size_mb,         "log_max_size_mb");
        get(cfg.log_max_files,           "log_max_files");

    } catch (const YAML::Exception& ex) {
        spdlog::warn("Config load failed ({}), using defaults: {}", path, ex.what());
    }
    return cfg;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    // Console logger
    spdlog::set_default_logger(spdlog::stdout_color_mt("console"));
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");

    std::string config_path = (argc > 1) ? argv[1] : "config/pipeline.yaml";
    auto cfg = load_config(config_path);

    spdlog::info("SensorExpress starting — config='{}'", config_path);

    // ── Lock memory before spawning RT threads ────────────────────────────
    try {
        sensor_express::lock_memory();
        spdlog::info("Memory locked (mlockall)");
    } catch (const std::exception& ex) {
        spdlog::warn("mlockall failed ({}); continuing without — RT latency may be higher", ex.what());
    }

    // ── Signal handlers ───────────────────────────────────────────────────
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGINT,  signal_handler);

    // ── Wire up processing callbacks ──────────────────────────────────────
    sensor_express::set_result_callback([&](const sensor_express::ProcessingResult& r) {
        sensor_express::record_reading_processed(r.processing_latency_ns);
        if (r.anomaly) {
            sensor_express::record_anomaly();
            bool high = (std::abs(r.z_score) >= cfg.alert_threshold);
            sensor_express::enqueue_alert(r, high);
        }
    });

    sensor_express::set_deadline_missed_callback([](const sensor_express::DeadlineMissed&) {
        sensor_express::record_deadline_miss();
    });

    // ── Start threads ─────────────────────────────────────────────────────
    std::thread mqtt_thread([&cfg] {
        sensor_express::run_mqtt_subscriber(cfg);
    });

    std::thread rt_thread([&cfg] {
        sensor_express::run_rt_processor(cfg);
    });

    std::thread alert_thread([&cfg] {
        sensor_express::run_alert_dispatcher(cfg);
    });

    std::thread metrics_thread([&cfg] {
        sensor_express::run_metrics_server(cfg);
    });

    spdlog::info("All threads started — waiting for SIGTERM/SIGINT");

    // ── Wait for shutdown ─────────────────────────────────────────────────
    while (!g_shutdown.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    spdlog::info("Shutdown signal received — stopping threads");

    sensor_express::stop_mqtt_subscriber();
    sensor_express::stop_rt_processor();
    sensor_express::stop_alert_dispatcher();
    sensor_express::stop_metrics_server();

    if (mqtt_thread.joinable())    mqtt_thread.join();
    if (rt_thread.joinable())      rt_thread.join();
    if (alert_thread.joinable())   alert_thread.join();
    if (metrics_thread.joinable()) metrics_thread.join();

    spdlog::info("SensorExpress stopped cleanly");
    return 0;
}
