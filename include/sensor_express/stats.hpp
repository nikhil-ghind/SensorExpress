#pragma once

#include <cmath>
#include <cstdint>
#include <stdexcept>

namespace sensor_express {

/**
 * Online rolling statistics using Welford's one-pass algorithm.
 *
 * Welford's method accumulates mean and variance in a single pass with
 * numerically stable updates.  No buffer is required, making it suitable
 * for the real-time processing loop.
 *
 * Reference: Welford (1962), "Note on a Method for Calculating Corrected
 * Sums of Squares and Products", Technometrics 4(3):419-420.
 */
class RollingStats {
public:
    explicit RollingStats(uint32_t warmup_samples = 30)
        : warmup_(warmup_samples) {}

    /** Incorporate a new observation. */
    void update(double x) {
        ++n_;
        double delta  = x - mean_;
        mean_ += delta / static_cast<double>(n_);
        double delta2 = x - mean_;
        M2_  += delta * delta2;
    }

    /** Population mean (returns 0 before any samples). */
    double mean() const { return mean_; }

    /**
     * Sample variance (Bessel-corrected).
     * Returns 0 when fewer than 2 samples have been seen.
     */
    double variance() const {
        if (n_ < 2) return 0.0;
        return M2_ / static_cast<double>(n_ - 1);
    }

    /** Sample standard deviation. */
    double stddev() const { return std::sqrt(variance()); }

    /** Number of samples seen so far. */
    uint64_t count() const { return n_; }

    /**
     * Z-score of a value relative to the running distribution.
     *
     * Returns 0 during the warm-up phase (not enough data for a stable
     * standard deviation estimate).
     */
    double z_score(double value) const {
        if (n_ < warmup_) return 0.0;
        double sd = stddev();
        if (sd < 1e-10) return 0.0;   // constant signal — no anomaly
        return (value - mean_) / sd;
    }

    /**
     * Returns true when |z_score(value)| >= threshold.
     *
     * The default threshold of 3.0 corresponds to ~0.27 % false-positive
     * rate under a Gaussian distribution.
     */
    bool is_anomaly(double value, double threshold = 3.0) const {
        return std::abs(z_score(value)) >= threshold;
    }

    /** Reset all accumulators. */
    void reset() {
        n_    = 0;
        mean_ = 0.0;
        M2_   = 0.0;
    }

private:
    uint64_t n_{0};
    double   mean_{0.0};
    double   M2_{0.0};
    uint32_t warmup_;
};

}  // namespace sensor_express
