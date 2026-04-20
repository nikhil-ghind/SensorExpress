/**
 * alert_dispatcher.cpp
 *
 * Dispatches anomaly alerts via HTTP POST webhook (libcurl) and
 * rotates structured logs via spdlog.
 *
 * High-priority:  z_score >= alert_threshold (default 4.0) → immediate POST
 * Low-priority:   z_score >= anomaly_threshold (default 3.0) → batched every 10 s
 */

#include "sensor_express/pipeline.hpp"

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace sensor_express {

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

struct AlertEvent {
    ProcessingResult result;
    bool             high_priority;
};

static std::queue<AlertEvent>     g_alert_queue;
static std::mutex                 g_alert_mutex;
static std::condition_variable    g_alert_cv;
static std::atomic<bool>          g_alert_running{false};

static std::shared_ptr<spdlog::logger> g_file_logger;

// ---------------------------------------------------------------------------
// libcurl helper
// ---------------------------------------------------------------------------

static size_t curl_discard(void* /*buf*/, size_t size, size_t nmemb, void* /*ud*/) {
    return size * nmemb;
}

static bool http_post_json(const std::string& url, const std::string& body) {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL,            url.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS,     body.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER,     headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,  curl_discard);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT,        5L);   // 5 s hard timeout
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 3L);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        spdlog::error("[alert] curl error: {}", curl_easy_strerror(res));
        return false;
    }
    return http_code >= 200 && http_code < 300;
}

// ---------------------------------------------------------------------------
// Payload builders
// ---------------------------------------------------------------------------

static std::string build_alert_payload(const ProcessingResult& r, bool high) {
    nlohmann::json j;
    j["sensor_id"]  = r.raw.sensor_id;
    j["value"]      = r.raw.value;
    j["unit"]       = r.raw.unit;
    j["z_score"]    = r.z_score;
    j["normalized"] = r.normalized;
    j["anomaly"]    = r.anomaly;
    j["priority"]   = high ? "high" : "low";
    j["timestamp_ns"] = r.raw.timestamp_ns;
    return j.dump();
}

static std::string build_batch_payload(const std::vector<AlertEvent>& batch) {
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& ev : batch) {
        nlohmann::json j;
        j["sensor_id"]  = ev.result.raw.sensor_id;
        j["value"]      = ev.result.raw.value;
        j["z_score"]    = ev.result.z_score;
        j["priority"]   = ev.high_priority ? "high" : "low";
        arr.push_back(j);
    }
    nlohmann::json root;
    root["alerts"] = arr;
    return root.dump();
}

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

void enqueue_alert(const ProcessingResult& result, bool high_priority) {
    {
        std::lock_guard<std::mutex> lk(g_alert_mutex);
        g_alert_queue.push({result, high_priority});
    }
    if (high_priority) g_alert_cv.notify_one();
}

void run_alert_dispatcher(const PipelineConfig& cfg) {
    // ── Set up rotating file logger ───────────────────────────────────────
    try {
        g_file_logger = spdlog::rotating_logger_mt(
            "anomaly_log",
            cfg.log_path,
            cfg.log_max_size_mb * 1024 * 1024,
            cfg.log_max_files);
        g_file_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    } catch (const spdlog::spdlog_ex& ex) {
        spdlog::warn("[alert] Could not open log file: {}", ex.what());
    }

    curl_global_init(CURL_GLOBAL_DEFAULT);
    g_alert_running = true;

    auto next_batch_time =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(cfg.alert_batch_interval_s);

    std::vector<AlertEvent> low_priority_batch;

    spdlog::info("[alert] Dispatcher started — webhook='{}'", cfg.webhook_url);

    while (g_alert_running.load()) {
        // Wait up to batch interval for a high-priority event
        std::unique_lock<std::mutex> lk(g_alert_mutex);
        g_alert_cv.wait_until(lk, next_batch_time, [&] {
            return !g_alert_queue.empty() || !g_alert_running.load();
        });

        // Drain queue
        std::vector<AlertEvent> events;
        while (!g_alert_queue.empty()) {
            events.push_back(std::move(g_alert_queue.front()));
            g_alert_queue.pop();
        }
        lk.unlock();

        // Process events
        for (const auto& ev : events) {
            // Log to rotating file
            if (g_file_logger) {
                g_file_logger->warn("anomaly sensor={} value={:.4f} z={:.2f} pri={}",
                    ev.result.raw.sensor_id, ev.result.raw.value,
                    ev.result.z_score, ev.high_priority ? "HIGH" : "LOW");
            }

            if (ev.high_priority && !cfg.webhook_url.empty()) {
                // Immediate POST for high-priority alerts
                std::string payload = build_alert_payload(ev.result, true);
                bool ok = http_post_json(cfg.webhook_url, payload);
                spdlog::info("[alert] High-priority POST {} {}",
                             ev.result.raw.sensor_id, ok ? "OK" : "FAILED");
            } else {
                low_priority_batch.push_back(ev);
            }
        }

        // Batch flush every alert_batch_interval_s
        auto now = std::chrono::steady_clock::now();
        if (now >= next_batch_time && !low_priority_batch.empty()) {
            if (!cfg.webhook_url.empty()) {
                std::string payload = build_batch_payload(low_priority_batch);
                bool ok = http_post_json(cfg.webhook_url, payload);
                spdlog::info("[alert] Batch POST {} events {}",
                             low_priority_batch.size(), ok ? "OK" : "FAILED");
            }
            low_priority_batch.clear();
            next_batch_time = now + std::chrono::seconds(cfg.alert_batch_interval_s);
        }
    }

    curl_global_cleanup();
    spdlog::info("[alert] Dispatcher stopped");
}

void stop_alert_dispatcher() {
    g_alert_running = false;
    g_alert_cv.notify_all();
}

}  // namespace sensor_express
