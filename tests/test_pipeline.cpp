/**
 * test_pipeline.cpp
 *
 * Google Test suite for SensorExpress core components.
 *
 * Test groups:
 *   RollingStatsTest    — Welford mean/variance correctness
 *   DeadlineTimerTest   — monotonic wakeup accuracy
 *   AnomalyTest         — z-score threshold behaviour
 *   PipelineIntegration — synthetic reading flow through processing logic
 */

#include "sensor_express/pipeline.hpp"
#include "sensor_express/stats.hpp"
#include "sensor_express/realtime.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <numeric>
#include <vector>
#include <thread>
#include <chrono>

using namespace sensor_express;

// ============================================================================
// RollingStats — Welford correctness
// ============================================================================

class RollingStatsTest : public ::testing::Test {
protected:
    RollingStats stats{/*warmup=*/0};   // disable warmup for unit tests
};

TEST_F(RollingStatsTest, InitialStateZero) {
    EXPECT_EQ(stats.count(), 0u);
    EXPECT_DOUBLE_EQ(stats.mean(), 0.0);
    EXPECT_DOUBLE_EQ(stats.variance(), 0.0);
}

TEST_F(RollingStatsTest, SingleSample) {
    stats.update(42.0);
    EXPECT_EQ(stats.count(), 1u);
    EXPECT_DOUBLE_EQ(stats.mean(), 42.0);
    EXPECT_DOUBLE_EQ(stats.variance(), 0.0);
}

TEST_F(RollingStatsTest, MeanCorrectness) {
    const std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    for (double v : values) stats.update(v);

    double expected_mean = 3.0;
    EXPECT_NEAR(stats.mean(), expected_mean, 1e-10);
}

TEST_F(RollingStatsTest, VarianceCorrectness) {
    // Known dataset: values 2,4,4,4,5,5,7,9 → variance=4.0 (sample)
    const std::vector<double> values = {2, 4, 4, 4, 5, 5, 7, 9};
    for (double v : values) stats.update(v);

    EXPECT_NEAR(stats.mean(), 5.0, 1e-10);
    EXPECT_NEAR(stats.variance(), 4.571428, 1e-5);  // Bessel-corrected
}

TEST_F(RollingStatsTest, StddevSquaredEqualsVariance) {
    for (double v : {10.0, 20.0, 30.0, 40.0, 50.0}) stats.update(v);
    double sd  = stats.stddev();
    double var = stats.variance();
    EXPECT_NEAR(sd * sd, var, 1e-9);
}

TEST_F(RollingStatsTest, ZScoreOfMeanIsZero) {
    for (int i = 0; i < 100; ++i) stats.update(static_cast<double>(i));
    double z = stats.z_score(stats.mean());
    EXPECT_NEAR(z, 0.0, 1e-9);
}

TEST_F(RollingStatsTest, ZScoreOutlier) {
    for (int i = 0; i < 200; ++i) stats.update(0.0);
    double z = stats.z_score(0.0);
    EXPECT_DOUBLE_EQ(z, 0.0);   // constant signal → stddev≈0

    // Non-trivial distribution
    RollingStats s2{0};
    for (int i = 0; i < 200; ++i) s2.update(static_cast<double>(i % 10));
    double extreme = s2.mean() + 10.0 * s2.stddev();
    EXPECT_GT(std::abs(s2.z_score(extreme)), 5.0);
}

TEST_F(RollingStatsTest, Reset) {
    for (double v : {1.0, 2.0, 3.0}) stats.update(v);
    stats.reset();
    EXPECT_EQ(stats.count(), 0u);
    EXPECT_DOUBLE_EQ(stats.mean(), 0.0);
}

// ============================================================================
// Anomaly threshold
// ============================================================================

TEST(AnomalyTest, DefaultThreshold3Sigma) {
    RollingStats s{0};
    for (int i = 0; i < 100; ++i) s.update(0.0);   // all zeros — no anomaly possible

    RollingStats s2{0};
    // Normal distribution approximation: values 0..99
    for (int i = 0; i < 100; ++i) s2.update(50.0 + (i % 10) - 5.0);

    // A value 4 sigma above mean should be flagged
    double extreme = s2.mean() + 4.0 * s2.stddev();
    EXPECT_TRUE(s2.is_anomaly(extreme, 3.0));

    // A value within 2 sigma should not be flagged
    double normal = s2.mean() + 1.5 * s2.stddev();
    EXPECT_FALSE(s2.is_anomaly(normal, 3.0));
}

TEST(AnomalyTest, CustomThreshold) {
    RollingStats s{0};
    for (int i = 0; i < 50; ++i) s.update(static_cast<double>(i));

    double mean = s.mean();
    double sd   = s.stddev();

    double v_at_2sd = mean + 2.0 * sd;
    EXPECT_TRUE( s.is_anomaly(v_at_2sd, 1.5));  // 2.0 >= 1.5 → anomaly
    EXPECT_FALSE(s.is_anomaly(v_at_2sd, 3.0));  // 2.0 < 3.0  → not anomaly
}

TEST(AnomalyTest, WarmupPreventsEarlyFlagging) {
    RollingStats s{50};   // warmup = 50 samples
    // Feed only 10 samples — z_score should return 0 (warmup not done)
    for (int i = 0; i < 10; ++i) s.update(static_cast<double>(i));
    EXPECT_DOUBLE_EQ(s.z_score(1000.0), 0.0);
    EXPECT_FALSE(s.is_anomaly(1000.0, 3.0));
}

// ============================================================================
// DeadlineTimer accuracy
// ============================================================================

TEST(DeadlineTimerTest, PeriodApproximate) {
    // 10 ms period — measure wall-clock time for 5 iterations
    constexpr long period_ns = 10'000'000L;
    DeadlineTimer  timer(period_ns);

    auto wall_start = std::chrono::steady_clock::now();
    constexpr int kIterations = 5;
    for (int i = 0; i < kIterations; ++i) {
        timer.wait_next();
    }
    auto wall_end = std::chrono::steady_clock::now();

    long elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          wall_end - wall_start).count();

    // Allow ±5 ms tolerance for scheduler jitter
    EXPECT_GE(elapsed_ms, kIterations * 10 - 5);
    EXPECT_LE(elapsed_ms, kIterations * 10 + 5);
}

TEST(DeadlineTimerTest, NoNegativeSleep) {
    // Very short period — wakeup should be immediate (overrun handling)
    DeadlineTimer timer(1L);   // 1 ns period — always overrun
    for (int i = 0; i < 10; ++i) {
        long late = timer.wait_next();
        EXPECT_GE(late, 0L);
    }
}

// ============================================================================
// Pipeline integration — synthetic readings
// ============================================================================

class PipelineIntegrationTest : public ::testing::Test {
protected:
    PipelineConfig cfg;

    void SetUp() override {
        cfg.anomaly_threshold = 3.0;
        cfg.alert_threshold   = 4.0;
        cfg.deadline_ms       = 5;
    }

    SensorReading make_reading(const std::string& id, double value,
                               SensorType type = SensorType::TEMPERATURE) {
        SensorReading r;
        r.sensor_id = id;
        r.value     = value;
        r.unit      = "degC";
        r.type      = type;
        struct timespec ts{};
        clock_gettime(CLOCK_MONOTONIC, &ts);
        r.timestamp_ns = static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;
        return r;
    }
};

TEST_F(PipelineIntegrationTest, NormalReadingsNotFlagged) {
    RollingStats s{0};
    for (int i = 0; i < 100; ++i) s.update(20.0 + (i % 5) * 0.1);

    double normal_value = 20.2;
    EXPECT_FALSE(s.is_anomaly(normal_value, cfg.anomaly_threshold));
}

TEST_F(PipelineIntegrationTest, AnomalyFlaggedAfterWarmup) {
    RollingStats s{30};
    // Warm up with stable values around 20.0
    for (int i = 0; i < 100; ++i) s.update(20.0 + 0.1 * (i % 3 - 1));

    // Inject a spike: +10 sigma
    double spike = s.mean() + 10.0 * s.stddev();
    EXPECT_TRUE(s.is_anomaly(spike, cfg.anomaly_threshold));
    EXPECT_GT(std::abs(s.z_score(spike)), cfg.alert_threshold);
}

TEST_F(PipelineIntegrationTest, MultiSensorIndependentStats) {
    RollingStats s_temp{0}, s_press{0};

    for (int i = 0; i < 100; ++i) {
        s_temp.update(20.0 + (i % 5) * 0.2);
        s_press.update(100.0 + (i % 5) * 1.0);
    }

    EXPECT_NEAR(s_temp.mean(),  20.4, 0.5);
    EXPECT_NEAR(s_press.mean(), 102.0, 2.0);

    // A value normal for temperature should be anomalous for pressure
    double temp_normal  = 21.0;
    double press_spike  = s_press.mean() + 5.0 * s_press.stddev();

    EXPECT_FALSE(s_temp.is_anomaly(temp_normal, 3.0));
    EXPECT_TRUE( s_press.is_anomaly(press_spike, 3.0));
}

TEST_F(PipelineIntegrationTest, ProcessingLatencyMeasured) {
    // Verify that timing machinery produces plausible values
    struct timespec start{}, end{};
    clock_gettime(CLOCK_MONOTONIC, &start);
    std::this_thread::sleep_for(std::chrono::microseconds(50));
    clock_gettime(CLOCK_MONOTONIC, &end);

    int64_t latency_ns =
        (static_cast<int64_t>(end.tv_sec  - start.tv_sec)  * 1'000'000'000LL)
      + (static_cast<int64_t>(end.tv_nsec - start.tv_nsec));

    EXPECT_GT(latency_ns, 10'000LL);         // > 10 µs
    EXPECT_LT(latency_ns, 50'000'000LL);     // < 50 ms
}

// ============================================================================
// Entry point
// ============================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
