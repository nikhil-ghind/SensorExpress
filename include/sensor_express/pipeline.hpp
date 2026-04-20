#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

namespace sensor_express {

enum class SensorType {
    TEMPERATURE,
    PRESSURE,
    HUMIDITY,
    VIBRATION,
    CURRENT,
    VOLTAGE,
    UNKNOWN
};

struct SensorReading {
    std::string sensor_id;
    double      value;
    std::string unit;
    int64_t     timestamp_ns;   // CLOCK_MONOTONIC nanoseconds
    SensorType  type{SensorType::UNKNOWN};
};

struct ProcessingResult {
    SensorReading raw;
    double        normalized{0.0};
    bool          anomaly{false};
    double        z_score{0.0};
    int64_t       processing_latency_ns{0};
};

struct DeadlineMissed {
    std::string sensor_id;
    int64_t     deadline_ns;
    int64_t     actual_ns;
};

struct PipelineConfig {
    // MQTT
    std::string mqtt_host{"localhost"};
    int         mqtt_port{1883};
    std::string mqtt_topic_pattern{"sensors/+/data"};
    std::string client_id{"sensor_express"};

    // RT
    int  rt_priority{80};
    int  rt_cpu{1};
    long deadline_ms{5};      // 5 ms hard deadline per cycle

    // Anomaly detection
    double anomaly_threshold{3.0};   // z-score threshold
    double alert_threshold{4.0};     // webhook alert threshold

    // Alert / webhook
    std::string webhook_url;
    int         alert_batch_interval_s{10};

    // Metrics
    int metrics_port{9090};

    // Logging
    std::string log_path{"logs/sensor_express.log"};
    size_t      log_max_size_mb{10};
    size_t      log_max_files{5};
};

}  // namespace sensor_express
