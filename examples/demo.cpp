// Push a few packets over a link that drops ~40% of attempts and print stats.
#include <chrono>
#include <cstdio>
#include <random>
#include <string>

#include "pdq/delivery_manager.hpp"

using namespace std::chrono_literals;

class LossyTransport : public pdq::ITransport {
public:
    bool send(const pdq::Packet& p) override {
        const bool ok = dist_(rng_) > 0.4;
        std::printf("  attempt -> packet %llu: %s\n",
                    static_cast<unsigned long long>(p.id),
                    ok ? "ACK" : "drop");
        return ok;
    }

private:
    std::mt19937 rng_{42};
    std::uniform_real_distribution<double> dist_{0.0, 1.0};
};

int main() {
    LossyTransport transport;
    pdq::RetryPolicy policy;
    policy.base_delay = 20ms;
    policy.max_attempts = 8;

    pdq::DeliveryManager mgr(transport, policy);
    for (std::uint64_t id = 1; id <= 5; ++id)
        mgr.enqueue(pdq::Packet{id, "telemetry-" + std::to_string(id)});

    mgr.wait_idle(5s);

    const auto s = mgr.stats();
    std::printf("\nenqueued=%llu delivered=%llu failed=%llu attempts=%llu\n",
                (unsigned long long)s.enqueued, (unsigned long long)s.delivered,
                (unsigned long long)s.failed, (unsigned long long)s.attempts);
    return 0;
}
