#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <time.h>
#include <errno.h>
#include <cstring>

namespace sensor_express {

/**
 * Elevate the calling thread to SCHED_FIFO real-time scheduling.
 *
 * @param priority  SCHED_FIFO priority [1,99].  80 is a sensible default
 *                  for a sensor processing thread; reserve 90+ for IRQ threads.
 * @throws std::runtime_error on failure (usually requires CAP_SYS_NICE or root)
 */
inline void set_realtime_priority(int priority) {
    struct sched_param param{};
    param.sched_priority = priority;
    if (sched_setscheduler(0, SCHED_FIFO, &param) != 0) {
        throw std::runtime_error(
            std::string("sched_setscheduler failed: ") + strerror(errno));
    }
}

/**
 * Pin the calling thread to a single logical CPU.
 *
 * Keeps the RT thread off shared CPUs, avoiding cache pollution and
 * scheduler jitter from non-RT work.
 *
 * @param cpu_id  Zero-based logical CPU index.
 */
inline void pin_to_cpu(int cpu_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        throw std::runtime_error(
            std::string("pthread_setaffinity_np failed: ") + strerror(errno));
    }
}

/**
 * Lock all current and future pages of the calling process into RAM.
 *
 * Prevents page-fault latency spikes inside the RT loop.  Must be called
 * before spawning RT threads so their stacks are pre-faulted.
 */
inline void lock_memory() {
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        throw std::runtime_error(
            std::string("mlockall failed: ") + strerror(errno));
    }
}

/**
 * Monotonic deadline timer using clock_nanosleep(CLOCK_MONOTONIC).
 *
 * Usage:
 *   DeadlineTimer t(1'000'000);   // 1 ms period
 *   while (running) {
 *       t.wait_next();            // blocks until next absolute deadline
 *       process();
 *   }
 *
 * Unlike relative sleep, absolute deadlines do not drift even when the
 * processing body takes variable time.
 */
class DeadlineTimer {
public:
    explicit DeadlineTimer(long period_ns) : period_ns_(period_ns) {
        clock_gettime(CLOCK_MONOTONIC, &next_);
    }

    /**
     * Block until the next absolute deadline, then advance the deadline by
     * one period.  If the caller is already past the deadline (overrun),
     * returns immediately and skips forward to avoid cascading overruns.
     *
     * @return  nanoseconds late (0 if on time).
     */
    long wait_next() {
        // Advance deadline by one period
        next_.tv_nsec += period_ns_;
        while (next_.tv_nsec >= 1'000'000'000L) {
            next_.tv_nsec -= 1'000'000'000L;
            next_.tv_sec  += 1;
        }

        struct timespec now{};
        clock_gettime(CLOCK_MONOTONIC, &now);
        long overrun_ns = timespec_diff_ns(now, next_);

        if (overrun_ns >= 0) {
            // Already past deadline — skip to stay real-time
            next_ = now;
            return overrun_ns;
        }

        while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next_, nullptr) == EINTR) {}
        return 0;
    }

    /** Return the nanosecond timestamp of the next scheduled wakeup. */
    int64_t next_deadline_ns() const {
        return static_cast<int64_t>(next_.tv_sec) * 1'000'000'000LL + next_.tv_nsec;
    }

private:
    struct timespec next_{};
    long            period_ns_;

    static long timespec_diff_ns(const struct timespec& a, const struct timespec& b) {
        return (static_cast<long>(a.tv_sec  - b.tv_sec)  * 1'000'000'000L)
             + (static_cast<long>(a.tv_nsec - b.tv_nsec));
    }
};

}  // namespace sensor_express
