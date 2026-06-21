#include <atomic>
#include <chrono>
#include <unordered_map>

#include "pdq/delivery_manager.hpp"
#include "simple_test.hpp"

using namespace std::chrono_literals;

namespace {

// Fails the first fail_n attempts per packet, then acks.
class FlakyTransport : public pdq::ITransport {
public:
    explicit FlakyTransport(int fail_n) : fail_n_(fail_n) {}

    bool send(const pdq::Packet& p) override {
        ++total_calls;
        const int seen = ++attempts_[p.id];
        return seen > fail_n_;
    }

    std::atomic<int> total_calls{0};

private:
    int fail_n_;
    std::unordered_map<std::uint64_t, int> attempts_;
};

class DeadTransport : public pdq::ITransport {
public:
    bool send(const pdq::Packet&) override {
        ++calls;
        return false;
    }
    std::atomic<int> calls{0};
};

pdq::RetryPolicy fast_policy() {
    pdq::RetryPolicy p;
    p.base_delay = 1ms;
    p.max_delay = 5ms;
    p.max_attempts = 5;
    return p;
}

}  // namespace

TEST_CASE(delivers_on_first_try) {
    FlakyTransport transport(0);
    pdq::DeliveryManager mgr(transport, fast_policy());
    mgr.enqueue(pdq::Packet{1, "hello"});

    CHECK(mgr.wait_idle(1s));
    CHECK(mgr.status(1) == pdq::Status::Delivered);
    CHECK(transport.total_calls == 1);
}

TEST_CASE(retries_then_succeeds) {
    FlakyTransport transport(2);  // fails twice, acks on the 3rd
    pdq::DeliveryManager mgr(transport, fast_policy());
    mgr.enqueue(pdq::Packet{42, "telemetry"});

    CHECK(mgr.wait_idle(1s));
    CHECK(mgr.status(42) == pdq::Status::Delivered);
    CHECK(transport.total_calls == 3);

    const auto s = mgr.stats();
    CHECK(s.delivered == 1);
    CHECK(s.failed == 0);
    CHECK(s.attempts == 3);
}

TEST_CASE(fails_after_exhausting_attempts) {
    DeadTransport transport;
    pdq::DeliveryManager mgr(transport, fast_policy());
    mgr.enqueue(pdq::Packet{7, "doomed"});

    CHECK(mgr.wait_idle(1s));
    CHECK(mgr.status(7) == pdq::Status::Failed);
    CHECK(transport.calls == 5);  // == max_attempts
}

TEST_CASE(handles_many_concurrent_packets) {
    FlakyTransport transport(1);
    pdq::DeliveryManager mgr(transport, fast_policy());

    constexpr int N = 200;
    for (int i = 0; i < N; ++i)
        mgr.enqueue(pdq::Packet{static_cast<std::uint64_t>(i), "p"});

    CHECK(mgr.wait_idle(5s));
    const auto s = mgr.stats();
    CHECK(s.enqueued == N);
    CHECK(s.delivered == N);
    CHECK(s.failed == 0);
    CHECK(s.attempts == 2 * N);
}

TEST_CASE(backoff_grows_and_caps) {
    pdq::RetryPolicy p;
    p.base_delay = 100ms;
    p.max_delay = 1000ms;
    p.multiplier = 2.0;
    CHECK(p.delay_for(0) == 100ms);
    CHECK(p.delay_for(1) == 200ms);
    CHECK(p.delay_for(2) == 400ms);
    CHECK(p.delay_for(3) == 800ms);
    CHECK(p.delay_for(4) == 1000ms);
    CHECK(p.delay_for(10) == 1000ms);
}

int main() { return st::run_all(); }
