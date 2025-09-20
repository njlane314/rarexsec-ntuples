#ifndef RAREXSEC_LOGGER_H
#define RAREXSEC_LOGGER_H

#include <chrono>
#include <exception>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

namespace proc::log {

namespace detail {
inline std::mutex &log_mutex() {
    static std::mutex m;
    return m;
}

inline std::string timestamp() {
    using clock = std::chrono::system_clock;
    auto now = clock::now();
    auto time = clock::to_time_t(now);
    std::tm tm{};
#if defined(_MSC_VER)
    localtime_s(&tm, &time);
#else
    localtime_r(&time, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

template <typename... Args>
inline std::string format_message(Args &&...args) {
    if constexpr (sizeof...(Args) == 0) {
        return {};
    } else {
        std::ostringstream oss;
        bool first = true;
        auto append = [&](auto &&value) {
            if (!first) {
                oss << ' ';
            }
            first = false;
            oss << std::forward<decltype(value)>(value);
        };
        (append(std::forward<Args>(args)), ...);
        return oss.str();
    }
}

template <typename... Args>
inline void write(std::ostream &os, Args &&...args) {
    std::lock_guard<std::mutex> lock(log_mutex());
    os << '[' << timestamp() << "] ";
    if constexpr (sizeof...(Args) > 0) {
        os << format_message(std::forward<Args>(args)...);
    }
    os << std::endl;
}

}

template <typename... Args>
inline void info(Args &&...args) {
    detail::write(std::clog, std::forward<Args>(args)...);
}

template <typename... Args>
[[noreturn]] inline void fatal(Args &&...args) {
    detail::write(std::clog, std::forward<Args>(args)...);
    std::terminate();
}

}

#endif
