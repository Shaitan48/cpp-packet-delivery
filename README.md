# pdq

Small C++17 library for at-least-once packet delivery over an unreliable link.
A background worker drains a queue, retries failed sends with exponential
backoff, and tracks each packet through `pending -> in-flight -> delivered/failed`.

I pulled this out of a larger monitoring project where an agent ships telemetry
to a backend and has to survive flaky links. Here it's trimmed down to the
delivery part with fake transports so it builds and runs standalone.

## Build

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
./build/pdq_demo        # optional
```

C++17 and CMake 3.16+.

## Usage

```cpp
class MyTransport : public pdq::ITransport {
    bool send(const pdq::Packet& p) override {
        // push p.payload over http/socket/whatever, return true on ack
    }
};

MyTransport t;
pdq::DeliveryManager mgr(t);
mgr.enqueue({1, "payload"});
mgr.wait_idle(std::chrono::seconds(5));
```

`send()` must return true only on a positive ack. Everything else is treated as
a failure and retried.

## Retry policy

Delay before retry `n` is `min(base * multiplier^n, max)`. Defaults: base 100ms,
multiplier 2, max 5s, 5 attempts. Override with `pdq::RetryPolicy`.

## Layout

```
include/pdq   headers
src           implementation
tests         tests + a tiny built-in harness
examples      runnable demo
```

## License

MIT.
