#include <atomic>
#include <chrono>
#include <cmath>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "pdq/delivery_manager.hpp"
#include "simple_test.hpp"

using namespace std::chrono_literals;

namespace {

// Имитация нестабильного сетевого транспорта (Flaky Transport).
// Первые fail_n попыток для каждого пакета завершаются неудачей, затем отправка проходит успешно.
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

// Абсолютно «мертвый» транспорт (всегда возвращает ошибку).
class DeadTransport : public pdq::ITransport {
public:
    bool send(const pdq::Packet&) override {
        ++calls;
        return false;
    }
    std::atomic<int> calls{0};
};

// Быстрая политика ретраев для ускорения выполнения тестов.
pdq::RetryPolicy fast_policy() {
    pdq::RetryPolicy p;
    p.base_delay = 1ms;
    p.max_delay = 5ms;
    p.max_attempts = 5;
    return p;
}

}  // namespace

// Проверяем идеальный сценарий: отправка без сбоев проходит с первой попытки.
TEST_CASE(delivers_on_first_try) {
    FlakyTransport transport(0);
    pdq::DeliveryManager mgr(transport, fast_policy());
    mgr.enqueue(pdq::Packet{1, "hello"});

    CHECK(mgr.wait_idle(1s));
    CHECK(mgr.status(1) == pdq::Status::Delivered);
    CHECK(transport.total_calls == 1);
}

// Тестируем повторные попытки: пакет должен пробиться через сбоящую сеть на 3-й раз.
TEST_CASE(retries_then_succeeds) {
    FlakyTransport transport(2);  // Первые две попытки ломаются, третья — успешная
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

// Убеждаемся, что при перманентном падении сети попытки прекращаются после исчерпания лимита.
TEST_CASE(fails_after_exhausting_attempts) {
    DeadTransport transport;
    pdq::DeliveryManager mgr(transport, fast_policy());
    mgr.enqueue(pdq::Packet{7, "doomed"});

    CHECK(mgr.wait_idle(1s));
    CHECK(mgr.status(7) == pdq::Status::Failed);
    CHECK(transport.calls == 5);  // Ожидаем ровно 5 вызовов (max_attempts)
}

// Нагрузочный тест: отправляем 200 пакетов одновременно в конкурентной среде.
TEST_CASE(handles_many_concurrent_packets) {
    FlakyTransport transport(1); // Каждый пакет падает один раз
    pdq::DeliveryManager mgr(transport, fast_policy());

    constexpr int N = 200;
    for (int i = 0; i < N; ++i)
        mgr.enqueue(pdq::Packet{static_cast<std::uint64_t>(i), "p"});

    CHECK(mgr.wait_idle(5s));
    const auto s = mgr.stats();
    CHECK(s.enqueued == N);
    CHECK(s.delivered == N);
    CHECK(s.failed == 0);
    CHECK(s.attempts == 2 * N); // 200 пакетов * 2 попытки каждый = 400 вызовов
}

// enqueue() после stop() не должен молча терять пакет - он должен упасть с исключением.
TEST_CASE(enqueue_after_stop_throws) {
    DeadTransport transport;
    pdq::DeliveryManager mgr(transport, fast_policy());
    mgr.stop();

    bool threw = false;
    try {
        mgr.enqueue(pdq::Packet{99, "too-late"});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

// Проверяем формулу экспоненциальной задержки и ее ограничение по max_delay.
TEST_CASE(backoff_grows_and_caps) {
    pdq::RetryPolicy p;
    p.base_delay = 100ms;
    p.max_delay = 1000ms;
    p.multiplier = 2.0;
    
    CHECK(p.delay_for(0) == 100ms);
    CHECK(p.delay_for(1) == 200ms);
    CHECK(p.delay_for(2) == 400ms);
    CHECK(p.delay_for(3) == 800ms);
    CHECK(p.delay_for(4) == 1000ms); // Ограничено max_delay
    CHECK(p.delay_for(10) == 1000ms); // Тоже ограничено max_delay
}


// jitter не должен ломать формулу backoff, только разбрасывать значение
// вокруг него в объявленных границах.
TEST_CASE(jitter_stays_within_bounds) {
    pdq::RetryPolicy p;
    p.base_delay = 100ms;
    p.max_delay = 1000ms;
    p.multiplier = 2.0;
    p.jitter = 0.2;

    for (int attempt = 1; attempt <= 5; ++attempt) {
        const double base = std::min(100.0 * std::pow(2.0, attempt), 1000.0);
        const auto lo = std::chrono::milliseconds(static_cast<long long>(base * 0.8));
        const auto hi = std::chrono::milliseconds(static_cast<long long>(base * 1.2));
        for (int i = 0; i < 20; ++i) {
            const auto d = p.delay_for(attempt);
            CHECK(d >= lo);
            CHECK(d <= hi);
        }
    }
}

// stop() должен мгновенно будить воркер, даже если он спит на длинном backoff.
TEST_CASE(stop_wakes_immediately_during_long_backoff) {
    DeadTransport transport;
    pdq::RetryPolicy p;
    p.base_delay = 10s;
    p.max_delay = 10s;
    p.max_attempts = 5;

    pdq::DeliveryManager mgr(transport, p);
    mgr.enqueue(pdq::Packet{1, "slow-backoff"});

    // Даем воркеру выполнить первую попытку и уйти в 10-секундный сон.
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (transport.calls.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(2ms);
    }
    CHECK(transport.calls.load() >= 1);

    const auto t0 = std::chrono::steady_clock::now();
    mgr.stop();
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    CHECK(elapsed < 500ms);
}

// Новый пакет с более ранним сроком должен вытеснять ожидание чужого длинного backoff.
TEST_CASE(earlier_packet_preempts_backoff_wait) {
    class RejectIdOneTransport : public pdq::ITransport {
    public:
        bool send(const pdq::Packet& pkt) override {
            if (pkt.id == 1) {
                ++id1_calls;
                return false;
            }
            return true;
        }
        std::atomic<int> id1_calls{0};
    };

    RejectIdOneTransport transport;
    pdq::RetryPolicy p;
    p.base_delay = 5s;
    p.max_delay = 5s;
    p.max_attempts = 3;

    pdq::DeliveryManager mgr(transport, p);
    mgr.enqueue(pdq::Packet{1, "fails-into-backoff"});

    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (transport.id1_calls.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(2ms);
    }
    CHECK(transport.id1_calls.load() >= 1);

    const auto t0 = std::chrono::steady_clock::now();
    mgr.enqueue(pdq::Packet{2, "urgent"});
    const auto poll_end = std::chrono::steady_clock::now() + 500ms;
    while (mgr.status(2) != pdq::Status::Delivered && std::chrono::steady_clock::now() < poll_end) {
        std::this_thread::sleep_for(2ms);
    }
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    CHECK(mgr.status(2) == pdq::Status::Delivered);
    CHECK(elapsed < 500ms);
    mgr.stop();
}

// Проверяем ограничение размера карты финальных статусов для долгих сессий 24/7.
TEST_CASE(bounds_terminal_status_history) {
    FlakyTransport transport(0);
    pdq::RetryPolicy p = fast_policy();
    p.max_status_history = 5;

    pdq::DeliveryManager mgr(transport, p);
    for (std::uint64_t i = 1; i <= 20; ++i) {
        mgr.enqueue(pdq::Packet{i, "p"});
    }
    CHECK(mgr.wait_idle(2s));

    int kept = 0;
    for (std::uint64_t i = 1; i <= 20; ++i) {
        if (mgr.status(i).has_value()) ++kept;
    }
    CHECK(kept == 5);
    CHECK(!mgr.status(1).has_value());
    CHECK(mgr.status(20) == pdq::Status::Delivered);
}

int main() { return st::run_all(); }
