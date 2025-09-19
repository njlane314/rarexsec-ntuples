#ifndef RAREXSEC_LOGGING_LOGGER_H
#define RAREXSEC_LOGGING_LOGGER_H

#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
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

inline std::string join_message() {
    return {};
}

template <typename First, typename... Rest>
inline std::string join_message(First &&first, Rest &&...rest) {
    std::ostringstream oss;
    oss << std::forward<First>(first);
    if constexpr (sizeof...(rest) > 0) {
        oss << ' ' << join_message(std::forward<Rest>(rest)...);
    }
    return oss.str();
}

template <typename... Args>
inline void write(std::ostream &os, const std::string &level, Args &&...args) {
    std::lock_guard<std::mutex> lock(log_mutex());
    os << '[' << timestamp() << "] [" << level << "] " << join_message(std::forward<Args>(args)...) << std::endl;
}

}

template <typename... Args>
inline void debug(Args &&...args) {
    detail::write(std::clog, "DEBUG", std::forward<Args>(args)...);
}

template <typename... Args>
inline void info(Args &&...args) {
    detail::write(std::clog, "INFO", std::forward<Args>(args)...);
}

template <typename... Args>
inline void warn(Args &&...args) {
    detail::write(std::clog, "WARN", std::forward<Args>(args)...);
}

template <typename... Args>
[[noreturn]] inline void fatal(Args &&...args) {
    std::string msg = detail::join_message(std::forward<Args>(args)...);
    detail::write(std::clog, "FATAL", msg);
    throw std::runtime_error(msg);
}

}

#endif
