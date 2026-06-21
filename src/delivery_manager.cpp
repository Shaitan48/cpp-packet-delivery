#include "pdq/delivery_manager.hpp"

#include <algorithm>
#include <cmath>

namespace pdq {

std::chrono::milliseconds RetryPolicy::delay_for(int attempt) const noexcept {
    if (attempt <= 0) return base_delay;
    const double scaled =
        static_cast<double>(base_delay.count()) * std::pow(multiplier, attempt);
    const double capped =
        std::min(scaled, static_cast<double>(max_delay.count()));
    return std::chrono::milliseconds(static_cast<long long>(capped));
}

DeliveryManager::DeliveryManager(ITransport& transport, RetryPolicy policy)
    : transport_(transport), policy_(policy) {
    worker_ = std::thread(&DeliveryManager::run, this);
}

DeliveryManager::~DeliveryManager() { stop(); }

void DeliveryManager::enqueue(Packet packet) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        statuses_[packet.id] = Status::Pending;
        queue_.push(Scheduled{std::move(packet), 0,
                              std::chrono::steady_clock::now()});
        ++outstanding_;
        ++stats_.enqueued;
    }
    cv_.notify_one();
}

std::optional<Status> DeliveryManager::status(std::uint64_t id) const {
    std::lock_guard<std::mutex> lock(mtx_);
    const auto it = statuses_.find(id);
    if (it == statuses_.end()) return std::nullopt;
    return it->second;
}

bool DeliveryManager::wait_idle(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mtx_);
    return cv_.wait_for(lock, timeout, [this] { return outstanding_ == 0; });
}

Stats DeliveryManager::stats() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return stats_;
}

void DeliveryManager::stop() {
    if (!running_.exchange(false)) return;  // idempotent
    cv_.notify_all();
    if (worker_.joinable()) worker_.join();
}

void DeliveryManager::run() {
    std::unique_lock<std::mutex> lock(mtx_);
    while (running_.load()) {
        if (queue_.empty()) {
            cv_.wait(lock, [this] { return !queue_.empty() || !running_.load(); });
            continue;
        }

        const auto due = queue_.top().due;
        if (due > std::chrono::steady_clock::now()) {
            cv_.wait_until(lock, due);
            continue;
        }

        Scheduled item = queue_.top();
        queue_.pop();
        statuses_[item.packet.id] = Status::InFlight;

        // Drop the lock around the network call so producers don't block.
        lock.unlock();
        const bool acked = transport_.send(item.packet);
        lock.lock();

        ++stats_.attempts;

        if (acked) {
            statuses_[item.packet.id] = Status::Delivered;
            ++stats_.delivered;
            if (outstanding_ > 0 && --outstanding_ == 0) cv_.notify_all();
            continue;
        }

        const int next_attempt = item.attempt + 1;
        if (next_attempt >= policy_.max_attempts) {
            statuses_[item.packet.id] = Status::Failed;
            ++stats_.failed;
            if (outstanding_ > 0 && --outstanding_ == 0) cv_.notify_all();
            continue;
        }

        item.attempt = next_attempt;
        item.due = std::chrono::steady_clock::now() +
                   policy_.delay_for(next_attempt);
        statuses_[item.packet.id] = Status::Pending;
        queue_.push(std::move(item));
    }
}

}  // namespace pdq
