/**
 * rt_processor.cpp
 *
 * Real-time sensor processing thread.
 *
 * Scheduling:  SCHED_FIFO priority 80, pinned to CPU 1
 * Period:      1 ms (configurable via PipelineConfig::deadline_ms)
 * Per-tick:    drain up to 50 readings from the MQTT queue,
 *              normalize values, compute z-scores via RollingStats,
 *              flag anomalies, emit DeadlineMissed if cycle >5 ms
 */

#include "sensor_express/pipeline.hpp"
#include "sensor_express/realtime.hpp"
#include "sensor_express/stats.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <functional>
#include <map>
#include <vector>
#include <ctime>

namespace sensor_express {

// Provided by mqtt_subscriber.cpp
size_t drain_queue(std::vector<SensorReading>& out, int timeout_ms);

// ---------------------------------------------------------------------------
// Callbacks set by main.cpp
// ---------------------------------------------------------------------------

static std::function<void(const ProcessingResult&)> g_on_result;
static std::function<void(const DeadlineMissed&)>   g_on_deadline_missed;
static std::atomic<bool>                             g_rt_running{false};

void set_result_callback(std::function<void(const ProcessingResult&)> cb) {
    g_on_result = std::move(cb);
}

void set_deadline_missed_callback(std::function<void(const DeadlineMissed&)> cb) {
    g_on_deadline_missed = std::move(cb);
}

// ---------------------------------------------------------------------------
// Per-sensor normalisation ranges (loaded from config in a real build;
// hard-coded here for clarity)
// ---------------------------------------------------------------------------

struct SensorRange { double lo; double hi; };

static SensorRange range_for(SensorType t) {
    switch (t) {
        case SensorType::TEMPERATURE: return {-40.0, 125.0};
        case SensorType::PRESSURE:    return {0.0, 300.0};
        case SensorType::HUMIDITY:    return {0.0, 100.0};
        case SensorType::VIBRATION:   return {0.0, 20.0};
        case SensorType::CURRENT:     return {0.0, 50.0};
        case SensorType::VOLTAGE:     return {0.0, 480.0};
        default:                      return {0.0, 1.0};
    }
}

inline double normalize(double value, const SensorRange& r) {
    if (r.hi <= r.lo) return 0.0;
    return (value - r.lo) / (r.hi - r.lo);
}

// ---------------------------------------------------------------------------
// RT processing loop
// ---------------------------------------------------------------------------

void run_rt_processor(const PipelineConfig& cfg) {
    // ── Real-time setup ───────────────────────────────────────────────────
    try {
        pin_to_cpu(cfg.rt_cpu);
        set_realtime_priority(cfg.rt_priority);
    } catch (const std::exception& ex) {
        spdlog::warn("[rt] RT setup failed (running without): {}", ex.what());
    }

    // ── Per-sensor statistics ─────────────────────────────────────────────
    std::map<std::string, RollingStats> stats_map;

    // ── Periodic timer (1 ms tick) ────────────────────────────────────────
    const long period_ns    = 1'000'000L;                          // 1 ms
    const long deadline_ns  = cfg.deadline_ms * 1'000'000L;        // 5 ms

    DeadlineTimer timer(period_ns);
    g_rt_running = true;

    std::vector<SensorReading> batch;
    batch.reserve(50);

    spdlog::info("[rt] Processor started — SCHED_FIFO prio={} cpu={}",
                 cfg.rt_priority, cfg.rt_cpu);

    while (g_rt_running.load()) {
        timer.wait_next();

        // Record tick start
        struct timespec tick_start{};
        clock_gettime(CLOCK_MONOTONIC, &tick_start);
        int64_t tick_start_ns =
            static_cast<int64_t>(tick_start.tv_sec) * 1'000'000'000LL
            + tick_start.tv_nsec;

        // ── Drain up to 50 readings ───────────────────────────────────────
        batch.clear();
        drain_queue(batch, /*timeout_ms=*/0);   // non-blocking for RT
        if (batch.size() > 50) batch.resize(50);

        // ── Process each reading ──────────────────────────────────────────
        for (const auto& r : batch) {
            struct timespec proc_start{};
            clock_gettime(CLOCK_MONOTONIC, &proc_start);

            auto& st = stats_map[r.sensor_id];
            st.update(r.value);

            ProcessingResult result;
            result.raw        = r;
            result.normalized = normalize(r.value, range_for(r.type));
            result.z_score    = st.z_score(r.value);
            result.anomaly    = st.is_anomaly(r.value, cfg.anomaly_threshold);

            struct timespec proc_end{};
            clock_gettime(CLOCK_MONOTONIC, &proc_end);
            result.processing_latency_ns =
                (static_cast<int64_t>(proc_end.tv_sec  - proc_start.tv_sec)  * 1'000'000'000LL)
              + (static_cast<int64_t>(proc_end.tv_nsec - proc_start.tv_nsec));

            if (g_on_result) g_on_result(result);
        }

        // ── Check deadline ────────────────────────────────────────────────
        struct timespec tick_end{};
        clock_gettime(CLOCK_MONOTONIC, &tick_end);
        int64_t tick_end_ns =
            static_cast<int64_t>(tick_end.tv_sec) * 1'000'000'000LL
            + tick_end.tv_nsec;
        int64_t elapsed_ns = tick_end_ns - tick_start_ns;

        if (elapsed_ns > deadline_ns) {
            DeadlineMissed dm;
            dm.sensor_id   = batch.empty() ? "none" : batch.back().sensor_id;
            dm.deadline_ns = deadline_ns;
            dm.actual_ns   = elapsed_ns;

            spdlog::warn("[rt] Deadline missed: elapsed={}us deadline={}us",
                         elapsed_ns / 1000, deadline_ns / 1000);

            if (g_on_deadline_missed) g_on_deadline_missed(dm);
        }
    }

    spdlog::info("[rt] Processor stopped");
}

void stop_rt_processor() {
    g_rt_running = false;
}

}  // namespace sensor_express
