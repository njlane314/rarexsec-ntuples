#ifndef RAREXSEC_SELECTION_QUERY_H
#define RAREXSEC_SELECTION_QUERY_H

#include <string>
#include <utility>

namespace proc {

class SelectionQuery {
  public:
    SelectionQuery() = default;
    explicit SelectionQuery(std::string filter) : filter_(std::move(filter)) {}

    const std::string &str() const noexcept { return filter_; }
    bool empty() const noexcept { return filter_.empty(); }

  private:
    std::string filter_;
};

}

#endif
