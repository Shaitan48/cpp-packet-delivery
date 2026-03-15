#include "pdq/packet.hpp"

namespace pdq {

const char* to_string(Status s) noexcept {
    switch (s) {
        case Status::Pending:   return "Pending";
        case Status::InFlight:  return "InFlight";
        case Status::Delivered: return "Delivered";
        case Status::Failed:    return "Failed";
    }
    return "Unknown";
}

}  // namespace pdq
