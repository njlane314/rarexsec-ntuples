#ifndef RUN_CONFIG_REGISTRY_H
#define RUN_CONFIG_REGISTRY_H

#include <map>
#include <stdexcept>
#include <string>

#include <rarexsec/data/RunConfig.h>

namespace analysis {

class RunConfigRegistry {
  public:
    void addConfig(RunConfig rc) {
        auto key = rc.label();
        if (configs_.count(key)) {
            throw std::runtime_error("Duplicate RunConfig label: " + key);
        }
        configs_.emplace(std::move(key), std::move(rc));
    }

    const RunConfig &get(const std::string &beam, const std::string &period) const {
        auto key = beam + ":" + period;
        auto it = configs_.find(key);
        if (it == configs_.end()) {
            throw std::out_of_range("RunConfig not found: " + key);
        }
        return it->second;
    }

    const std::map<std::string, RunConfig> &all() const noexcept { return configs_; }

  private:
    std::map<std::string, RunConfig> configs_;
};

}

#endif
