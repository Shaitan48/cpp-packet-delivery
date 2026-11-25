#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>

#include "pdq/delivery_manager.hpp"

namespace {

class CountingTransport final : public pdq::ITransport {
public:
    bool send(const pdq::Packet&) override {
        sent_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    std::uint64_t sent() const noexcept {
        return sent_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<std::uint64_t> sent_{0};
};

}  // namespace

int main() {
    constexpr std::uint64_t kPackets = 50000;

    CountingTransport transport;
    pdq::RetryPolicy policy;
    policy.max_status_history = 10000;

    pdq::DeliveryManager manager(transport, policy);

    const auto start = std::chrono::steady_clock::now();
    for (std::uint64_t i = 1; i <= kPackets; ++i) {
        manager.enqueue(pdq::Packet{i, "telemetry-payload"});
    }
    const bool idle = manager.wait_idle(std::chrono::seconds(30));
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);

    const double seconds = static_cast<double>(elapsed.count()) / 1e6;
    const double rate = seconds > 0.0 ? static_cast<double>(kPackets) / seconds : 0.0;

    std::cout << "idle=" << (idle ? "true" : "false")
              << " packets=" << kPackets
              << " sent=" << transport.sent()
              << " elapsed_ms=" << (elapsed.count() / 1000.0)
              << " throughput_pkt_per_sec=" << static_cast<std::uint64_t>(rate)
              << '\n';

    return idle && transport.sent() == kPackets ? 0 : 1;
}
