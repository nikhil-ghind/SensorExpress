/**
 * metrics_server.cpp
 *
 * Exposes Prometheus-format metrics via a minimal HTTP server using the
 * httplib single-header library.  Scraped by Prometheus at /metrics.
 *
 * Counters:
 *   sensor_express_readings_processed_total
 *   sensor_express_deadline_misses_total
 *   sensor_express_anomalies_detected_total
 *
 * Histogram:
 *   sensor_express_processing_latency_ns_bucket{le="..."} — latency per reading
 */

#include "sensor_express/pipeline.hpp"

#include <httplib.h>
#include <spdlog/spdlog.h>

#include <array>
#include <atomic>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace sensor_express {

// ---------------------------------------------------------------------------
// Metric storage
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_readings_total{0};
static std::atomic<uint64_t> g_deadline_misses_total{0};
static std::atomic<uint64_t> g_anomalies_total{0};

struct LatencyHistogram {
    // Bucket upper bounds in nanoseconds
    static constexpr std::array<int64_t, 8> kBounds = {
        1'000,       // 1 µs
        5'000,       // 5 µs
        10'000,      // 10 µs
        50'000,      // 50 µs
        100'000,     // 100 µs
        500'000,     // 500 µs
        1'000'000,   // 1 ms
        5'000'000    // 5 ms
    };

    std::array<std::atomic<uint64_t>, 8> buckets{};
    std::atomic<uint64_t> count{0};
    std::atomic<int64_t>  sum_ns{0};

    void observe(int64_t latency_ns) {
        ++count;
        sum_ns += latency_ns;
        for (size_t i = 0; i < kBounds.size(); ++i) {
            if (latency_ns <= kBounds[i]) {
                ++buckets[i];
            }
        }
    }
} g_latency_hist;

// ---------------------------------------------------------------------------
// Metric update helpers (called from other threads)
// ---------------------------------------------------------------------------

void record_reading_processed(int64_t latency_ns) {
    ++g_readings_total;
    g_latency_hist.observe(latency_ns);
}

void record_deadline_miss() {
    ++g_deadline_misses_total;
}

void record_anomaly() {
    ++g_anomalies_total;
}

// ---------------------------------------------------------------------------
// Prometheus text format serialiser
// ---------------------------------------------------------------------------

static std::string render_metrics() {
    std::ostringstream ss;

    // Counter: readings_processed_total
    ss << "# HELP sensor_express_readings_processed_total "
          "Total sensor readings processed by the RT pipeline\n"
       << "# TYPE sensor_express_readings_processed_total counter\n"
       << "sensor_express_readings_processed_total "
       << g_readings_total.load() << "\n\n";

    // Counter: deadline_misses_total
    ss << "# HELP sensor_express_deadline_misses_total "
          "Number of RT processing cycles that exceeded the deadline\n"
       << "# TYPE sensor_express_deadline_misses_total counter\n"
       << "sensor_express_deadline_misses_total "
       << g_deadline_misses_total.load() << "\n\n";

    // Counter: anomalies_detected_total
    ss << "# HELP sensor_express_anomalies_detected_total "
          "Total anomalous readings flagged (|z-score| >= threshold)\n"
       << "# TYPE sensor_express_anomalies_detected_total counter\n"
       << "sensor_express_anomalies_detected_total "
       << g_anomalies_total.load() << "\n\n";

    // Histogram: processing_latency_ns
    ss << "# HELP sensor_express_processing_latency_ns "
          "Per-reading processing latency in nanoseconds\n"
       << "# TYPE sensor_express_processing_latency_ns histogram\n";

    for (size_t i = 0; i < LatencyHistogram::kBounds.size(); ++i) {
        ss << "sensor_express_processing_latency_ns_bucket{le=\""
           << LatencyHistogram::kBounds[i] << "\"} "
           << g_latency_hist.buckets[i].load() << "\n";
    }
    ss << "sensor_express_processing_latency_ns_bucket{le=\"+Inf\"} "
       << g_latency_hist.count.load() << "\n";
    ss << "sensor_express_processing_latency_ns_sum "
       << g_latency_hist.sum_ns.load() << "\n";
    ss << "sensor_express_processing_latency_ns_count "
       << g_latency_hist.count.load() << "\n\n";

    return ss.str();
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

static httplib::Server g_server;
static std::atomic<bool> g_metrics_running{false};

void run_metrics_server(const PipelineConfig& cfg) {
    g_metrics_running = true;

    g_server.Get("/metrics", [](const httplib::Request&, httplib::Response& res) {
        res.set_content(render_metrics(), "text/plain; version=0.0.4; charset=utf-8");
    });

    g_server.Get("/healthz", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("ok\n", "text/plain");
    });

    spdlog::info("[metrics] HTTP server listening on :{}", cfg.metrics_port);
    g_server.listen("0.0.0.0", cfg.metrics_port);
    spdlog::info("[metrics] Server stopped");
}

void stop_metrics_server() {
    g_server.stop();
    g_metrics_running = false;
}

}  // namespace sensor_express
