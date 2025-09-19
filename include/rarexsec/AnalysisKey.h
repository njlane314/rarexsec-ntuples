#ifndef ANALYSIS_KEY_H
#define ANALYSIS_KEY_H

#include <cstddef>
#include <functional>
#include <string>
#include <utility>

namespace proc {

class SampleKey {
  public:
    SampleKey() = default;
    explicit SampleKey(std::string value) : value_(std::move(value)) {}

    const std::string &str() const noexcept { return value_; }
    const char *c_str() const noexcept { return value_.c_str(); }

    bool operator==(const SampleKey &other) const noexcept { return value_ == other.value_; }
    bool operator!=(const SampleKey &other) const noexcept { return !(*this == other); }
    bool operator<(const SampleKey &other) const noexcept { return value_ < other.value_; }

  private:
    std::string value_;
};

struct SampleKeyHash {
    std::size_t operator()(const SampleKey &key) const noexcept { return std::hash<std::string>{}(key.str()); }
};

}

namespace std {
template <> struct hash<proc::SampleKey> {
    std::size_t operator()(const proc::SampleKey &key) const noexcept { return proc::SampleKeyHash{}(key); }
};
}

#endif
