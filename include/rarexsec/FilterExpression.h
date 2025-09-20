#ifndef FILTER_EXPRESSION_H
#define FILTER_EXPRESSION_H

#include <string>
#include <utility>

namespace proc {

class FilterExpression {
  public:
    FilterExpression() = default;
    explicit FilterExpression(std::string filter) : filter_(std::move(filter)) {}

    const std::string &str() const noexcept { return filter_; }
    bool empty() const noexcept { return filter_.empty(); }

  private:
    std::string filter_;
};

}

#endif
