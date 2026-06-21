#pragma once

// Minimal test harness - just enough to avoid pulling in a dependency.
// If the suite grows, swap for Catch2 or GoogleTest.

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

namespace st {

struct Case {
    std::string name;
    std::function<void()> fn;
};

inline std::vector<Case>& registry() {
    static std::vector<Case> cases;
    return cases;
}

inline int& failures() {
    static int n = 0;
    return n;
}

struct Registrar {
    Registrar(const std::string& name, std::function<void()> fn) {
        registry().push_back({name, std::move(fn)});
    }
};

inline int run_all() {
    int failed_cases = 0;
    for (auto& c : registry()) {
        const int before = failures();
        c.fn();
        const bool ok = failures() == before;
        std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", c.name.c_str());
        if (!ok) ++failed_cases;
    }
    std::printf("\n%zu test(s), %d failed\n", registry().size(), failed_cases);
    return failed_cases == 0 ? 0 : 1;
}

}  // namespace st

#define TEST_CASE(name)                                                    \
    static void name();                                                    \
    static ::st::Registrar registrar_##name(#name, name);                  \
    static void name()

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            ++::st::failures();                                            \
            std::printf("  CHECK failed: %s (%s:%d)\n", #cond, __FILE__,   \
                        __LINE__);                                         \
        }                                                                  \
    } while (0)
