#include "pdq/delivery_manager.hpp"

#include <algorithm>
#include <cmath>
#include <random>
#include <stdexcept>

namespace pdq {

std::chrono::milliseconds RetryPolicy::delay_for(int attempt) const noexcept {
    if (attempt <= 0) return base_delay;
    // Формула backoff: base * multiplier^attempt.
    // Приводим к double для расчетов степени, затем кастуем обратно.
    const double scaled =
        static_cast<double>(base_delay.count()) * std::pow(multiplier, attempt);
    const double capped =
        std::min(scaled, static_cast<double>(max_delay.count()));

    if (jitter <= 0.0) {
        return std::chrono::milliseconds(static_cast<long long>(capped));
    }

    // rng статический на поток, чтобы не тащить его как поле и не платить
    // за него, пока jitter не включен.
    thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> spread(1.0 - jitter, 1.0 + jitter);
    const double jittered = std::max(0.0, capped * spread(rng));
    return std::chrono::milliseconds(static_cast<long long>(jittered));
}

DeliveryManager::DeliveryManager(ITransport& transport, RetryPolicy policy)
    : transport_(transport), policy_(policy) {
    // Воркер запускается в самом конце инициализации, когда все остальные поля
    // гарантированно созданы (особенно mutex и condition_variable).
    worker_ = std::thread(&DeliveryManager::run, this);
}

DeliveryManager::~DeliveryManager() { 
    stop(); 
}

void DeliveryManager::enqueue(Packet packet) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!running_.load()) {
            // Воркер уже остановлен - без этой проверки пакет тихо оседал бы
            // в очереди навсегда, а wait_idle() потом никогда бы не дождался.
            // Заметил на живом агенте: shutdown гонится с последней пачкой
            // enqueue() из другого потока, и пакет просто терялся без следа.
            throw std::runtime_error("DeliveryManager::enqueue called after stop()");
        }
        statuses_[packet.id] = Status::Pending;
        queue_.push(Scheduled{std::move(packet), 0,
                              std::chrono::steady_clock::now()});
        ++outstanding_;
        ++stats_.enqueued;
    }
    cv_.notify_one(); // Будим воркера, если он заснул на ожиданиях
}

std::optional<Status> DeliveryManager::status(std::uint64_t id) const {
    std::lock_guard<std::mutex> lock(mtx_);
    const auto it = statuses_.find(id);
    if (it == statuses_.end()) return std::nullopt;
    return it->second;
}

bool DeliveryManager::wait_idle(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mtx_);
    // Ждем на condition variable, пока количество пакетов в обработке не упадет до нуля.
    return cv_.wait_for(lock, timeout, [this] { return outstanding_ == 0; });
}

Stats DeliveryManager::stats() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return stats_;
}

void DeliveryManager::stop() {
    // exchange гарантирует атомарный сброс флага и предотвращает двойной вызов cv.notify/thread.join.
    if (!running_.exchange(false)) return; 
    
    cv_.notify_all(); // Будим воркера, чтобы он вышел из цикла
    if (worker_.joinable()) worker_.join();
}

void DeliveryManager::run() {
    std::unique_lock<std::mutex> lock(mtx_);
    while (running_.load()) {
        if (queue_.empty()) {
            // Если работы нет, спим до первого входящего пакета или сигнала об остановке
            cv_.wait(lock, [this] { return !queue_.empty() || !running_.load(); });
            continue;
        }

        const auto due = queue_.top().due;
        const auto now = std::chrono::steady_clock::now();
        if (due > now) {
            // Если время отправки верхнего пакета еще не пришло, засыпаем на разницу во времени
            cv_.wait_until(lock, due);
            continue;
        }

        // Вытаскиваем пакет. 
        // Важно: top() в priority_queue возвращает const reference, что запрещает std::move.
        // Чтобы избежать дорогого копирования std::string payload из Packet, используем
        // классический C++ хак с const_cast. Это безопасно, так как мы тут же делаем pop().
        Scheduled item = std::move(const_cast<Scheduled&>(queue_.top()));
        queue_.pop();
        statuses_[item.packet.id] = Status::InFlight;

        // КРИТИЧЕСКИЙ МОМЕНТ: отпускаем блокировку мьютекса перед отправкой по сети!
        // Иначе сетевой вызов send() (который может висеть по таймауту секундами)
        // заблокирует любые вызовы enqueue() или status() из внешних потоков.
        lock.unlock();
        const bool acked = transport_.send(item.packet);
        lock.lock(); // Снова захватываем мьютекс для обновления состояния

        ++stats_.attempts;

        if (acked) {
            statuses_[item.packet.id] = Status::Delivered;
            ++stats_.delivered;
            // Если это был последний обрабатываемый пакет, будим потоки, ждущие в wait_idle()
            if (outstanding_ > 0 && --outstanding_ == 0) cv_.notify_all();
            continue;
        }

        // Обработка неудачной попытки отправки
        const int next_attempt = item.attempt + 1;
        if (next_attempt >= policy_.max_attempts) {
            statuses_[item.packet.id] = Status::Failed;
            ++stats_.failed;
            if (outstanding_ > 0 && --outstanding_ == 0) cv_.notify_all();
            continue;
        }

        // Если попытки не исчерпаны, считаем новую задержку и возвращаем пакет обратно в очередь
        item.attempt = next_attempt;
        item.due = std::chrono::steady_clock::now() +
                   policy_.delay_for(next_attempt);
        statuses_[item.packet.id] = Status::Pending;
        queue_.push(std::move(item));
    }
}

}  // namespace pdq
