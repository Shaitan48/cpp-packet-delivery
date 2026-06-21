#pragma once

#include "pdq/packet.hpp"

namespace pdq {

// The actual wire (HTTP, socket, message queue - whatever).
// send() returns true only if the packet was acknowledged; on timeout,
// error or nack return false and it will be retried.
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual bool send(const Packet& packet) = 0;
};

}  // namespace pdq
