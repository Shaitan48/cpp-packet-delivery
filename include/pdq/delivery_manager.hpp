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

// Настройки повторной отправки пакетов с экспоненциальной задержкой.
struct RetryPolicy {
    int max_attempts = 5;                        // Лимит попыток для одного пакета, после чего он помечается как Failed.
    std::chrono::milliseconds base_delay{100};   // Начальная задержка перед первой повторной отправкой.
    std::chrono::milliseconds max_delay{5000};   // Верхний предел задержки (чтобы backoff не рос бесконечно).
    double multiplier = 2.0;                     // Коэффициент экспоненциального роста (умножитель для каждой попытки).

    // Рассчитывает задержку для конкретной попытки: min(base_delay * multiplier^attempt, max_delay).
    std::chrono::milliseconds delay_for(int attempt) const noexcept;
};

// Метрики работы менеджера доставки. Полезно для мониторинга flaky-соединений.
struct Stats {
    std::uint64_t enqueued = 0;   // Всего поставлено пакетов в очередь.
    std::uint64_t delivered = 0;  // Успешно доставлено (получен ACK).
    std::uint64_t failed = 0;     // Окончательно не доставлено (исчерпаны попытки).
    std::uint64_t attempts = 0;   // Суммарное количество сетевых вызовов send().
};

// Менеджер надежной доставки пакетов по принципу "at-least-once" (как минимум один раз).
// Внутренний фоновый поток (worker) последовательно разгребает очередь; методы enqueue()
// и status() полностью потокобезопасны и могут вызываться конкурентно.
class DeliveryManager {
public:
    explicit DeliveryManager(ITransport& transport, RetryPolicy policy = {});
    ~DeliveryManager();

    // Запрещаем копирование и присваивание, чтобы случайно не размножить потоки и мьютексы.
    DeliveryManager(const DeliveryManager&) = delete;
    DeliveryManager& operator=(const DeliveryManager&) = delete;

    // Потокобезопасно ставит пакет в очередь на отправку. Будит воркера, если тот спал.
    void enqueue(Packet packet);

    // Возвращает текущий статус пакета по ID (Pending -> InFlight -> Delivered/Failed).
    std::optional<Status> status(std::uint64_t id) const;

    // Блокирует вызывающий поток до тех пор, пока очередь не опустеет (все пакеты
    // либо доставлены, либо исчерпали попытки), либо пока не истечет таймаут.
    // Возвращает true, если все задачи обработаны до таймаута.
    bool wait_idle(std::chrono::milliseconds timeout);

    // Возвращает копию текущей статистики.
    Stats stats() const;

    // Останавливает рабочий поток. Метод идемпотентен, автоматически вызывается в деструкторе.
    void stop();

private:
    // Структура для приоритетной очереди воркера.
    struct Scheduled {
        Packet packet;
        int attempt = 0;
        std::chrono::steady_clock::time_point due;

        // Оператор "больше" нужен для std::greater в priority_queue,
        // чтобы в голове очереди всегда оказывался элемент с наименьшим временем отправки (min-heap).
        bool operator>(const Scheduled& o) const { return due > o.due; }
    };

    // Основной цикл воркера, работающий в фоновом потоке.
    void run();

    ITransport& transport_;
    RetryPolicy policy_;

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    
    std::priority_queue<Scheduled, std::vector<Scheduled>, std::greater<>> queue_;
    std::unordered_map<std::uint64_t, Status> statuses_;
    
    std::size_t outstanding_ = 0; // Число пакетов, находящихся в процессе обработки (Pending / InFlight).
    Stats stats_;
    std::atomic<bool> running_{true};
    std::thread worker_;
};

}  // namespace pdq
