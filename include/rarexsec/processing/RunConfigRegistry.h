#ifndef RAREXSEC_PROCESSING_RUN_CONFIG_REGISTRY_H
#define RAREXSEC_PROCESSING_RUN_CONFIG_REGISTRY_H

#include <map>
#include <stdexcept>
#include <string>

#include <rarexsec/processing/RunConfig.h>

namespace analysis {

class RunConfigRegistry {
  public:
    void addConfig(RunConfig config);

    const RunConfig &get(const std::string &beam, const std::string &period) const;

    const std::map<std::string, RunConfig> &all() const noexcept { return configs_; }

  private:
    std::map<std::string, RunConfig> configs_;
};

}

#endif
