#pragma once

#include <cstdint>
#include <string>

namespace pdq {

enum class Status {
    Pending,
    InFlight,
    Delivered,
    Failed
};

const char* to_string(Status s) noexcept;

struct Packet {
    std::uint64_t id = 0;
    std::string payload;

    Packet() = default;
    Packet(std::uint64_t id, std::string payload)
        : id(id), payload(std::move(payload)) {}
};

}  // namespace pdq
