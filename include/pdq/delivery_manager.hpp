#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "pdq/packet.hpp"
#include "pdq/transport.hpp"

namespace pdq {

struct RetryPolicy {
    int max_attempts = 5;
    std::chrono::milliseconds base_delay{100};
    std::chrono::milliseconds max_delay{5000};
    double multiplier = 2.0;

    std::chrono::milliseconds delay_for(int attempt) const noexcept;
};

struct Stats {
    std::uint64_t enqueued = 0;
    std::uint64_t delivered = 0;
    std::uint64_t failed = 0;
    std::uint64_t attempts = 0;  // total send() calls
};

// At-least-once delivery with bounded exponential-backoff retries.
// A single worker thread owns the queue; enqueue() is safe from any thread.
class DeliveryManager {
public:
    explicit DeliveryManager(ITransport& transport, RetryPolicy policy = {});
    ~DeliveryManager();

    DeliveryManager(const DeliveryManager&) = delete;
    DeliveryManager& operator=(const DeliveryManager&) = delete;

    void enqueue(Packet packet);
    std::optional<Status> status(std::uint64_t id) const;

    // Block until every packet is delivered or failed, or until timeout.
    // Returns true if the queue drained in time.
    bool wait_idle(std::chrono::milliseconds timeout);

    Stats stats() const;
    void stop();

private:
    struct Scheduled {
        Packet packet;
        int attempt = 0;
        std::chrono::steady_clock::time_point due;
        bool operator>(const Scheduled& o) const { return due > o.due; }
    };

    void run();

    ITransport& transport_;
    RetryPolicy policy_;

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::priority_queue<Scheduled, std::vector<Scheduled>, std::greater<>> queue_;
    std::unordered_map<std::uint64_t, Status> statuses_;
    std::size_t outstanding_ = 0;
    Stats stats_;
    std::atomic<bool> running_{true};
    std::thread worker_;
};

}  // namespace pdq
